use std::{collections::{BTreeMap, HashMap, HashSet}, io::Cursor, sync::{Arc, atomic::{AtomicU64, Ordering}}, time::Duration};
use async_raft::{AppData, AppDataResponse, NodeId, Raft, RaftNetwork, RaftStorage, raft::{AppendEntriesRequest, AppendEntriesResponse, ClientWriteRequest, Entry, EntryPayload, InstallSnapshotRequest, InstallSnapshotResponse, MembershipConfig, VoteRequest, VoteResponse}, storage::{CurrentSnapshotData, HardState, InitialState}};
use tokio::sync::RwLock;
use tracing::{Level, debug, error};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use anyhow::{Context, Result};

fn get_append_state() -> &'static AtomicU64{
    ::lazy_static::lazy_static! {
        static ref INSTANCE: AtomicU64 = AtomicU64::new(0);
    }
    &*INSTANCE
}

#[tracing::instrument(level = TRACE_LEVEL)]
fn add_append_state() {
    let n = get_append_state().fetch_add(1, Ordering::Relaxed);
    debug!("state {:?}", n+1);
}

#[tracing::instrument(level = TRACE_LEVEL)]
fn check_append_state() {
    let n = get_append_state().load(Ordering::Relaxed);
    debug!("state {:?}", n);
    if n < 2 {
        error!("wrong append state [{}]", n);
    }
}

fn get_nodes() -> &'static Nodes {
    ::lazy_static::lazy_static! {
        static ref INSTANCE: Nodes = Nodes::default();
    }
    &*INSTANCE
}


#[derive(Serialize, Deserialize, Debug, Clone)]
struct ClientRequest {
    req_id: u64,
}

impl AppData for ClientRequest {}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientResponse{}

impl AppDataResponse for ClientResponse {}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoreSnapshot {
    /// The last index covered by this snapshot.
    pub index: u64,
    /// The term of the last index covered by this snapshot.
    pub term: u64,
    /// The last memberhsip config included in this snapshot.
    pub membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}


#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct StateMachine {
    last_applied_log: u64,
    replicates: u64,
}

    
#[derive(Clone, Debug, thiserror::Error)]
pub enum ShutdownError { }

const ERR_INCONSISTENT_LOG: &str = "a query was received which was expecting data to be in place which does not exist in the log";

const TRACE_LEVEL: tracing::Level = tracing::Level::DEBUG;

#[derive(Default)]
struct LocalStore {
    id: NodeId,
    log: RwLock<BTreeMap<u64, Entry<ClientRequest>>>,
    sm: RwLock<StateMachine>,
    hs: RwLock<Option<HardState>>,
    current_snapshot: RwLock<Option<StoreSnapshot>>,
}

impl LocalStore {
    fn new(id: NodeId) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    async fn get_membership(&self) -> Result<MembershipConfig> {
        let log = self.log.read().await;
        let cfg_opt = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
            EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
            _ => None,
        });
        Ok(match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.id),
        })
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for LocalStore {

    type Snapshot = Cursor<Vec<u8>>;
    
    type ShutdownError = ShutdownError;

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        debug!("");
        self.get_membership().await
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn get_initial_state(&self) -> Result<async_raft::storage::InitialState> {
        debug!("");
        let mut hs = self.hs.write().await;
        let new = InitialState::new_initial(self.id);
        *hs = Some(new.hard_state.clone());
        Ok(new)
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, hs), fields(local = %self.id))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        debug!("hard state = {:?}", hs);
        *self.hs.write().await = Some(hs.clone());
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        debug!("");
        if start > stop {
            tracing::error!("invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        debug!("");
        if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
            tracing::error!("invalid request, start > stop");
            return Ok(());
        }
        let mut log = self.log.write().await;

        // If a stop point was specified, delete from start until the given stop point.
        if let Some(stop) = stop.as_ref() {
            for key in start..*stop {
                log.remove(&key);
            }
            return Ok(());
        }
        // Else, just split off the remainder.
        log.split_off(&start);
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, fields(local = %self.id), skip(self, entry))]
    async fn append_entry_to_log(&self, entry: &Entry<ClientRequest>) -> Result<()> {
        debug!("{:?}", entry);

        if let EntryPayload::Normal(_r) = &entry.payload {
            add_append_state();
        }
        
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, entries), fields(local = %self.id))]
    async fn replicate_to_log(&self, entries: &[Entry<ClientRequest>]) -> Result<()> {
        debug!("{:?}", entries);
        
        for entry in entries {
            if let EntryPayload::Normal(_r) = &entry.payload {
                add_append_state();
            }
        }
        
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, data), fields(local = %self.id))]
    async fn apply_entry_to_state_machine(&self, index: &u64, data: &ClientRequest) -> Result<ClientResponse> {
        debug!("{:?}", data);
        check_append_state();
        let mut sm = self.sm.write().await;
        sm.last_applied_log = *index;
        Ok(ClientResponse{})
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, entries), fields(local = %self.id))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        debug!("{:?}", entries);
        check_append_state();
        let mut sm = self.sm.write().await;
        for (index, _data) in entries {
            sm.last_applied_log = **index;
        }
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        debug!("");
        let (data, last_applied_log);
        {
            // Serialize the data of the state machine.
            let sm = self.sm.read().await;
            data = serde_json::to_vec(&*sm)?;
            last_applied_log = sm.last_applied_log;
        } // Release state machine read lock.

        let membership_config;
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let log = self.log.read().await;
            membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > last_applied_log)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));
        } // Release log read lock.

        let snapshot_bytes: Vec<u8>;
        let term;
        {
            let mut log = self.log.write().await;
            let mut current_snapshot = self.current_snapshot.write().await;
            term = log
                .get(&last_applied_log)
                .map(|entry| entry.term)
                .ok_or_else(|| anyhow::anyhow!(ERR_INCONSISTENT_LOG))?;
            *log = log.split_off(&last_applied_log);
            log.insert(
                last_applied_log,
                Entry::new_snapshot_pointer(last_applied_log, term, "".into(), membership_config.clone()),
            );

            let snapshot = StoreSnapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data,
            };
            snapshot_bytes = serde_json::to_vec(&snapshot)?;
            *current_snapshot = Some(snapshot);
        } // Release log & snapshot write locks.

        tracing::trace!({ snapshot_size = snapshot_bytes.len() }, "log compaction complete");
        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership_config.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        debug!("");
        Ok((String::from(""), Box::new(Cursor::new(Vec::new())))) // Snapshot IDs are insignificant to this storage engine.
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self, snapshot), fields(local = %self.id))]
    async fn finalize_snapshot_installation(
        &self, index: u64, term: u64, delete_through: Option<u64>, id: String, snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        debug!({ snapshot_size = snapshot.get_ref().len() }, "decoding snapshot for installation");

        let raw = serde_json::to_string_pretty(snapshot.get_ref().as_slice())?;
        println!("JSON SNAP:\n{}", raw);
        let new_snapshot: StoreSnapshot = serde_json::from_slice(snapshot.get_ref().as_slice())?;
        // Update log.
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let mut log = self.log.write().await;
            let membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > index)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));

            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }
            log.insert(index, Entry::new_snapshot_pointer(index, term, id, membership_config));
        }

        // Update the state machine.
        {
            let new_sm: StateMachine = serde_json::from_slice(&new_snapshot.data)?;
            let mut sm = self.sm.write().await;
            *sm = new_sm;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = TRACE_LEVEL, skip(self), fields(local = %self.id))]
    async fn get_current_snapshot(&self) -> Result<Option<async_raft::storage::CurrentSnapshotData<Self::Snapshot>>> {
        debug!("");
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = serde_json::to_vec(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}


#[derive(Default)]
struct Router {

}

#[async_trait]
impl RaftNetwork<ClientRequest> for Router {
    async fn append_entries(&self, target: NodeId, rpc: AppendEntriesRequest<ClientRequest>) -> Result<AppendEntriesResponse> {
        let node = get_nodes().get(&target).await.unwrap();
        let r = node.raft.append_entries(rpc).await?;
        Ok(r)
    }

    async fn install_snapshot(&self, target: NodeId, rpc: InstallSnapshotRequest) -> Result<InstallSnapshotResponse> {
        let node = get_nodes().get(&target).await.unwrap();
        let r = node.raft.install_snapshot(rpc).await?;
        Ok(r)
    }

    async fn vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let node = get_nodes().get(&target).await.unwrap();
        let r = node.raft.vote(rpc).await?;
        Ok(r)
    }
}

fn build_raft_config(name: String) -> Result<Arc<async_raft::Config>> {
    let builder = async_raft::Config::build(name)
    // .election_timeout_min(50_000_000_000)
    // .election_timeout_max(100_000_000_000)
    // .heartbeat_interval(300_000)
    ;
    let config = Arc::new(builder.validate().context("failed to build Raft config")?);
    Ok(config)
}

struct Node {
    id: NodeId,
    raft: Raft<ClientRequest, ClientResponse, Router, LocalStore>, 
}

impl Node {
    fn new(id: NodeId) -> Result<Self> {
        let router = Arc::new(Router::default());
        let store = Arc::new(LocalStore::new(id));
        let config = build_raft_config(format!("cluster"))?;
        let raft = Raft::new(store.id, config, router.clone(), store.clone());
        Ok( Self {
            id,
            raft,
        })
    }
}

#[derive(Default)]
struct Nodes {
    data: Arc<RwLock<HashMap<NodeId, Arc<Node>>>>,
}

impl Nodes {
    async fn init(&self, n: u64) -> Result<()> {
        let mut nodes = self.data.write().await;
        for nid in 0..n {
            let node = Node::new(nid)?;
            nodes.insert(node.id, Arc::new(node));
        }
        Ok(())
    }

    async fn get(&self, id: &NodeId) -> Option<Arc<Node>> {
        let nodes = self.data.read().await;
        if let Some(node) = nodes.get(id) {
            Some(node.clone())
        } else {
            None
        }
    }
}

#[tracing::instrument(level = TRACE_LEVEL)]
async fn run_main() -> Result<()> {
    get_nodes().init(3).await?;

    let mut members = HashSet::new();

    let node0;

    {
        let nid0 = 0;
        node0 = get_nodes().get(&nid0).await.unwrap();
        members.insert(nid0);
        node0.raft.initialize(members.clone()).await?;
    
        loop {
            let r = node0.raft.current_leader().await;
            if let Some(nid) = r {
                assert!(nid == nid0);
                debug!("current leader {}", nid);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    {
        let nid1 = 1;
        node0.raft.add_non_voter(nid1).await?;
        members.insert(nid1);
        node0.raft.change_membership(members.clone()).await?;
    }

    {
        let nid2 = 2;
        node0.raft.add_non_voter(nid2).await?;
        members.insert(nid2);
        node0.raft.change_membership(members.clone()).await?;
    }

    node0.raft.client_write(ClientWriteRequest::new(ClientRequest{req_id: 1})).await?;
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()>{
    tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(Level::DEBUG)
        .init();

        run_main().await
}
