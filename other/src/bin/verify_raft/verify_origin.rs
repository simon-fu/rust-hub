
    use anyhow::Result;
    use tokio::time::Instant;
    use tracing::{debug, error};
    use std::{collections::HashMap, sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, atomic::{AtomicU64, Ordering}}, time::Duration};
    use raft::{Config, RaftState, RawNode, StateRole, Storage, eraftpb::{ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message, Snapshot, SnapshotMetadata}};
    use protobuf::Message as PbMessage;
    // use prost::Message as PbMessage;

    type NodeId = u64;
    const TRACE_LEVEL: tracing::Level = tracing::Level::DEBUG;

    #[derive(Clone)]
    pub struct MemStorageCore {
        raft_state: RaftState,
        // entries[i] has raft log position i+snapshot.get_metadata().index
        entries: Vec<Entry>,
        // Metadata of the last snapshot received.
        snapshot_metadata: SnapshotMetadata,
        // If it is true, the next snapshot will return a
        // SnapshotTemporarilyUnavailable error.
        trigger_snap_unavailable: bool,
    }
    
    impl Default for MemStorageCore {
        fn default() -> MemStorageCore {
            MemStorageCore {
                raft_state: Default::default(),
                entries: vec![],
                // Every time a snapshot is applied to the storage, the metadata will be stored here.
                snapshot_metadata: Default::default(),
                // When starting from scratch populate the list with a dummy entry at term zero.
                trigger_snap_unavailable: false,
            }
        }
    }
    
    impl MemStorageCore {
        /// Saves the current HardState.
        pub fn set_hardstate(&mut self, hs: HardState) {
            self.raft_state.hard_state = hs;
        }
    
        /// Get the hard state.
        #[allow(dead_code)]
        pub fn hard_state(&self) -> &HardState {
            &self.raft_state.hard_state
        }
    
        /// Get the mut hard state.
        pub fn mut_hard_state(&mut self) -> &mut HardState {
            &mut self.raft_state.hard_state
        }
    
        /// Commit to an index.
        ///
        /// # Panics
        ///
        /// Panics if there is no such entry in raft logs.
        #[allow(dead_code)]
        pub fn commit_to(&mut self, index: u64) -> Result<()> {
            assert!(
                self.has_entry_at(index),
                "commit_to {} but the entry does not exist",
                index
            );
    
            let diff = (index - self.entries[0].index) as usize;
            self.raft_state.hard_state.commit = index;
            self.raft_state.hard_state.term = self.entries[diff].term;
            Ok(())
        }
    
        /// Saves the current conf state.
        pub fn set_conf_state(&mut self, cs: ConfState) {
            self.raft_state.conf_state = cs;
        }
    
        #[allow(dead_code)]
        #[inline]
        fn has_entry_at(&self, index: u64) -> bool {
            !self.entries.is_empty() && index >= self.first_index() && index <= self.last_index()
        }
    
        fn first_index(&self) -> u64 {
            match self.entries.first() {
                Some(e) => e.index,
                None => self.snapshot_metadata.index + 1,
            }
        }
    
        fn last_index(&self) -> u64 {
            match self.entries.last() {
                Some(e) => e.index,
                None => self.snapshot_metadata.index,
            }
        }
    
        /// Overwrites the contents of this Storage object with those of the given snapshot.
        ///
        /// # Panics
        ///
        /// Panics if the snapshot index is less than the storage's first index.
        pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> raft::Result<()> {
            let mut meta = snapshot.take_metadata();
            let index = meta.index;
    
            if self.first_index() > index {
                return Err(raft::Error::Store(raft::StorageError::SnapshotOutOfDate));
            }
    
            self.snapshot_metadata = meta.clone();
    
            self.raft_state.hard_state.term = std::cmp::max(self.raft_state.hard_state.term, meta.term);
            self.raft_state.hard_state.commit = index;
            self.entries.clear();
    
            // Update conf states.
            self.raft_state.conf_state = meta.take_conf_state();
            Ok(())
        }
    
        fn snapshot(&self) -> Snapshot {
            let mut snapshot = Snapshot::default();
    
            // We assume all entries whose indexes are less than `hard_state.commit`
            // have been applied, so use the latest commit index to construct the snapshot.
            // TODO: This is not true for async ready.
            let meta = snapshot.mut_metadata();
            meta.index = self.raft_state.hard_state.commit;
            meta.term = match meta.index.cmp(&self.snapshot_metadata.index) {
                std::cmp::Ordering::Equal => self.snapshot_metadata.term,
                std::cmp::Ordering::Greater => {
                    let offset = self.entries[0].index;
                    self.entries[(meta.index - offset) as usize].term
                }
                std::cmp::Ordering::Less => {
                    panic!(
                        "commit {} < snapshot_metadata.index {}",
                        meta.index, self.snapshot_metadata.index
                    );
                }
            };
    
            meta.set_conf_state(self.raft_state.conf_state.clone());
            snapshot
        }
    
        /// Discards all log entries prior to compact_index.
        /// It is the application's responsibility to not attempt to compact an index
        /// greater than RaftLog.applied.
        ///
        /// # Panics
        ///
        /// Panics if `compact_index` is higher than `Storage::last_index(&self) + 1`.
        #[allow(dead_code)]
        pub fn compact(&mut self, compact_index: u64) -> Result<()> {
            if compact_index <= self.first_index() {
                // Don't need to treat this case as an error.
                return Ok(());
            }
    
            if compact_index > self.last_index() + 1 {
                panic!(
                    "compact not received raft logs: {}, last index: {}",
                    compact_index,
                    self.last_index()
                );
            }
    
            if let Some(entry) = self.entries.first() {
                let offset = compact_index - entry.index;
                self.entries.drain(..offset as usize);
            }
            Ok(())
        }
    
        /// Append the new entries to storage.
        ///
        /// # Panics
        ///
        /// Panics if `ents` contains compacted entries, or there's a gap between `ents` and the last
        /// received entry in the storage.
        pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
            if ents.is_empty() {
                return Ok(());
            }
            if self.first_index() > ents[0].index {
                panic!(
                    "overwrite compacted raft logs, compacted: {}, append: {}",
                    self.first_index() - 1,
                    ents[0].index,
                );
            }
            if self.last_index() + 1 < ents[0].index {
                panic!(
                    "raft logs should be continuous, last index: {}, new appended: {}",
                    self.last_index(),
                    ents[0].index,
                );
            }
    
            // Remove all entries overwritten by `ents`.
            let diff = ents[0].index - self.first_index();
            self.entries.drain(diff as usize..);
            self.entries.extend_from_slice(&ents);
            Ok(())
        }
    
        /// Commit to `idx` and set configuration to the given states. Only used for tests.
        #[allow(dead_code)]
        pub fn commit_to_and_set_conf_states(&mut self, idx: u64, cs: Option<ConfState>) -> Result<()> {
            self.commit_to(idx)?;
            if let Some(cs) = cs {
                self.raft_state.conf_state = cs;
            }
            Ok(())
        }
    
        /// Trigger a SnapshotTemporarilyUnavailable error.
        #[allow(dead_code)]
        pub fn trigger_snap_unavailable(&mut self) {
            self.trigger_snap_unavailable = true;
        }
    }


    #[derive(Clone, Default)]
    pub struct MemStorage {
        id: NodeId,
        core: Arc<RwLock<MemStorageCore>>,
    }

    impl MemStorage {
        /// Returns a new memory storage value.
        pub fn new(id: NodeId) -> MemStorage {
            MemStorage {
                id,
                ..Default::default()
            }
        }

        /// Create a new `MemStorage` with a given `Config`. The given `Config` will be used to
        /// initialize the storage.
        ///
        /// You should use the same input to initialize all nodes.
        pub fn new_with_conf_state<T>(id: NodeId, conf_state: T) -> MemStorage
        where
            ConfState: From<T>,
        {
            let store = MemStorage::new(id);
            store.initialize_with_conf_state(conf_state);
            store
        }

        /// Initialize a `MemStorage` with a given `Config`.
        ///
        /// You should use the same input to initialize all nodes.
        pub fn initialize_with_conf_state<T>(&self, conf_state: T)
        where
            ConfState: From<T>,
        {
            assert!(!self.initial_state().unwrap().initialized());
            let mut core = self.wl();
            // Setting initial state is very important to build a correct raft, as raft algorithm
            // itself only guarantees logs consistency. Typically, you need to ensure either all start
            // states are the same on all nodes, or new nodes always catch up logs by snapshot first.
            //
            // In practice, we choose the second way by assigning non-zero index to first index. Here
            // we choose the first way for historical reason and easier to write tests.
            core.raft_state.conf_state = ConfState::from(conf_state);
        }

        /// Opens up a read lock on the storage and returns a guard handle. Use this
        /// with functions that don't require mutation.
        pub fn rl(&self) -> RwLockReadGuard<'_, MemStorageCore> {
            self.core.read().unwrap()
        }

        /// Opens up a write lock on the storage and returns guard handle. Use this
        /// with functions that take a mutable reference to self.
        pub fn wl(&self) -> RwLockWriteGuard<'_, MemStorageCore> {
            self.core.write().unwrap()
        }
    }

    
    impl Storage for MemStorage {

        fn initial_state(&self) -> raft::Result<RaftState> {
            debug!("Storage[node={}] initial_state", self.id);
            Ok(self.rl().raft_state.clone())
        }

        fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> raft::Result<Vec<Entry>> {
            let max_size = max_size.into();
            debug!("Storage[node={}] entries, low {}, high {}, max_size {:?}", self.id, low, high, max_size);
            let core = self.rl();
            if low < core.first_index() {
                return Err(raft::Error::Store(raft::StorageError::Compacted));
            }

            if high > core.last_index() + 1 {
                panic!(
                    "index out of bound (last: {}, high: {})",
                    core.last_index() + 1,
                    high
                );
            }

            let offset = core.entries[0].index;
            let lo = (low - offset) as usize;
            let hi = (high - offset) as usize;
            let mut ents = core.entries[lo..hi].to_vec();
            raft::util::limit_size(&mut ents, max_size);
            Ok(ents)
        }

        fn term(&self, idx: u64) -> raft::Result<u64> {
            debug!("Storage[node={}] term, idx {}", self.id, idx);

            let core = self.rl();
            if idx == core.snapshot_metadata.index {
                return Ok(core.snapshot_metadata.term);
            }

            let offset = core.first_index();
            if idx < offset {
                return Err(raft::Error::Store(raft::StorageError::Compacted));
            }

            if idx > core.last_index() {
                return Err(raft::Error::Store(raft::StorageError::Unavailable));
            }
            Ok(core.entries[(idx - offset) as usize].term)
        }

        fn first_index(&self) -> raft::Result<u64> {
            Ok(self.rl().first_index())
        }

        fn last_index(&self) -> raft::Result<u64> {
            Ok(self.rl().last_index())
        }

        fn snapshot(&self, request_index: u64) -> raft::Result<Snapshot> {
            debug!("Storage[node={}] snapshot, request_index {}", self.id, request_index);
            let mut core = self.wl();
            if core.trigger_snap_unavailable {
                core.trigger_snap_unavailable = false;
                Err(raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable))
            } else {
                let mut snap = core.snapshot();
                if snap.get_metadata().index < request_index {
                    snap.mut_metadata().index = request_index;
                }
                Ok(snap)
            }
        }
    }

    fn build_raft_config(id: NodeId) -> Config {
        let cfg = Config {
            // The unique ID for the Raft node.
            id: id,
            // Election tick is for how long the follower may campaign again after
            // it doesn't receive any message from the leader.
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // The max size limits the max size of each appended message. Mostly, 1 MB is enough.
            max_size_per_msg: 1024 * 1024 * 1024,
            // Max inflight msgs that the leader sends messages to follower without
            // receiving ACKs.
            max_inflight_msgs: 256,
            // The Raft applied index.
            // You need to save your applied index when you apply the committed Raft logs.
            applied: 0,
            ..Default::default()
        };
        cfg
    }

    fn get_append_state() -> &'static AtomicU64{
        ::lazy_static::lazy_static! {
            static ref INSTANCE: AtomicU64 = AtomicU64::new(0);
        }
        &*INSTANCE
    }
    
    #[tracing::instrument(level = TRACE_LEVEL)]
    fn add_append_state(node_id: NodeId) {
        let n = get_append_state().fetch_add(1, Ordering::Relaxed);
        debug!("state {:?}", n+1);
    }
    
    #[tracing::instrument(level = TRACE_LEVEL)]
    fn check_append_state(node_id: NodeId) {
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

    struct NodeInner {
        raft_group: RawNode<MemStorage>,
        last_apply_index: u64,
    }

    struct Node {
        id: NodeId, 
        inner: RwLock<NodeInner>,
    }

    impl Node {
        fn new(id: NodeId) -> Result<Self> {
            let config = build_raft_config(id);
            let storage = MemStorage::new_with_conf_state(id, ConfState::from((vec![1], vec![])));
            let raft_group = RawNode::with_default_logger(&config, storage).unwrap();
            Ok( Self {
                id,
                inner: RwLock::new(NodeInner {
                    raft_group,
                    last_apply_index: 0,
                }),
            })
        }

        fn handle_committed_entries(&self, inner: &mut NodeInner, committed_entries: Vec<Entry>) {
            if committed_entries.len() == 0 {
                return;
            }
            
            debug!("Node[{}]:handle_committed_entries: {:?}", self.id, committed_entries);

            for entry in committed_entries {
                // Mostly, you need to save the last apply index to resume applying
                // after restart. Here we just ignore this because we use a Memory storage.
                inner.last_apply_index = entry.index;
    
                if entry.data.is_empty() {
                    // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                    continue;
                }
                
                match entry.get_entry_type() {
                    EntryType::EntryNormal => {
                        let data = std::str::from_utf8(&entry.data).unwrap();
                        debug!("Node[{}]:handle_committed_entries:EntryNormal: {}", self.id, data);
                        check_append_state(self.id);
                    },
                    EntryType::EntryConfChange => {
                        let mut cc = ConfChange::default();
                        cc.merge_from_bytes(&entry.data).unwrap();
                        let cs = inner.raft_group.apply_conf_change(&cc).unwrap();
                        debug!("Node[{}]:handle_committed_entries:EntryConfChange: {:?}, {:?}", self.id, cc, cs);
                        inner.raft_group.store().wl().set_conf_state(cs);

                    },
                    EntryType::EntryConfChangeV2 => {

                    },
                }
    
                // TODO: handle EntryConfChange
            }
        }

        fn handle_message(&self, msg: Message) -> Result<()> {
            debug!("Node[{}]:handle_message: {:?}", self.id, msg);
            let mut inner = self.inner.write().unwrap();
            inner.raft_group.step(msg)?;
            Ok(())
        }

        fn send_out_messages(&self, msgs: Vec<Message>) -> Result<()> {
            for msg in msgs {
                // Send messages to other peers.
                let target = get_nodes().get(&msg.to).unwrap();
                target.handle_message(msg)?;
            }
            Ok(())
        }

        fn do_tick(&self, inner: &mut NodeInner) -> Result<()> {
            inner.raft_group.tick();

            if !inner.raft_group.has_ready() {
                return Ok(());
            }
            let store = inner.raft_group.raft.raft_log.store.clone();
        
            // Get the `Ready` with `RawNode::ready` interface.
            let mut ready = inner.raft_group.ready();
        
            if !ready.messages().is_empty() {
                // Send out the messages come from the node.
                self.send_out_messages(ready.take_messages())?;
            }
        
            if !ready.snapshot().is_empty() {
                // This is a snapshot, we need to apply the snapshot at first.
                store.wl().apply_snapshot(ready.snapshot().clone()).unwrap();
            }
        
            self.handle_committed_entries(inner, ready.take_committed_entries());
        
            if !ready.entries().is_empty() {
                // Append entries to the Raft log.
                store.wl().append(ready.entries()).unwrap();

                for e in ready.entries() {
                    if e.data.is_empty() {
                        // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                        continue;
                    }

                    // debug!("Node[{}]:append_entry: {:?}", self.id, e);
                    match e.get_entry_type() {
                        EntryType::EntryNormal => {
                            debug!("Node[{}]:append_entry:EntryNormal {:?}", self.id, e);
                            add_append_state(self.id);
                        },
                        EntryType::EntryConfChange => {},
                        EntryType::EntryConfChangeV2 => {},
                    }
                }
            }
        
            if let Some(hs) = ready.hs() {
                // Raft HardState changed, and we need to persist it.
                store.wl().set_hardstate(hs.clone());
            }
        
            if !ready.persisted_messages().is_empty() {
                // Send out the persisted messages come from the node.
                self.send_out_messages(ready.take_persisted_messages())?;
            }

            // Advance the Raft.
            let mut light_rd = inner.raft_group.advance(ready);
            // Update commit index.
            if let Some(commit) = light_rd.commit_index() {
                store.wl().mut_hard_state().set_commit(commit);
            }
            // Send out the messages.
            self.send_out_messages(light_rd.take_messages())?;
            // Apply all committed entries.
            self.handle_committed_entries(inner, light_rd.take_committed_entries());
            // Advance the apply index.
            inner.raft_group.advance_apply();


            Ok(())
        }

        fn process_tick(&self) -> Result<()> {
            debug!("Node[{}]:process_tick", self.id);
            let mut inner = self.inner.write().unwrap();
            self.do_tick(&mut inner)
        }

        fn role(&self) -> Result<StateRole> {
            let inner = self.inner.read().unwrap();
            Ok(inner.raft_group.raft.state) 
        }

        fn conf_state(&self) -> Result<ConfState> {
            let inner = self.inner.read().unwrap();
            let state = inner.raft_group.store().rl().raft_state.conf_state.clone();
            Ok(state)
        }

        fn add_node(&self, node_id: NodeId) -> Result<()> {
            let mut conf_change = ConfChange::default();
            conf_change.node_id = node_id;
            conf_change.set_change_type(ConfChangeType::AddNode);
            let mut inner = self.inner.write().unwrap();
            inner.raft_group.propose_conf_change(vec![], conf_change)?;
            Ok(())
        }

        fn propose(&self, data: String) -> Result<()> {
            let mut inner = self.inner.write().unwrap();
            inner.raft_group.propose(vec![], data.into_bytes())?;
            Ok(())
        }
    }

    #[derive(Default)]
    struct Nodes {
        data: Arc<RwLock<HashMap<NodeId, Arc<Node>>>>,
    }
    
    impl Nodes {
        async fn init(&self, n: u64) -> Result<()> {
            let mut nodes = self.data.write().unwrap();
            for nid in 1..n+1 {
                let node = Node::new(nid)?;
                nodes.insert(node.id, Arc::new(node));
            }
            Ok(())
        }
    
        fn get(&self, id: &NodeId) -> Option<Arc<Node>> {
            let nodes = self.data.read().unwrap();
            if let Some(node) = nodes.get(id) {
                Some(node.clone())
            } else {
                None
            }
        }
    }

    #[tracing::instrument(level = TRACE_LEVEL)]
    pub async fn run_main() -> Result<()> {
        get_nodes().init(3).await?;

        let node1 = get_nodes().get(&1).unwrap();
        let node2 = get_nodes().get(&2).unwrap();
        let node3 = get_nodes().get(&3).unwrap();
        debug!("created nodes");

        let timeout = Duration::from_millis(100);

        while node1.role()? != StateRole::Leader {
            node1.process_tick()?;
        }
        debug!("node1 become leader");

        node1.add_node(node2.id)?;
        while !node1.conf_state()?.voters.contains(&node2.id) {
            node1.process_tick()?;
        }
        debug!("node1 added node2");

        node1.add_node(node3.id)?;
        while !node1.conf_state()?.voters.contains(&node3.id) {
            node1.process_tick()?;
            node2.process_tick()?;
        }
        debug!("node1 added node3");
        
        node1.propose(format!("hello"))?;

        let mut next_time = Instant::now() + timeout;
        for _ in 0..10 {
            tokio::select! {
                _r = tokio::time::sleep_until(next_time) => {
                    next_time = Instant::now() + timeout;
                    node1.process_tick()?;
                    node2.process_tick()?;
                    node3.process_tick()?;
                }
            }
        }

        Ok(())
    }
    