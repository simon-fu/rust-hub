
// Your system allocator is suboptimal. If you switch to jemalloc your memory usage will drop by a lot.
#[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;
// static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
// static GLOBAL: TCMalloc = TCMalloc;

pub struct Packet { pub id: String }

async fn burn_memory(){
    // let packet_size = 1000 as usize;
    let max_packets = 1000*1000 as usize;

    // let mut v : Vec<tokio::task::JoinHandle<()>> = Vec::new();
    // let (tx, _rx) = tokio::sync::broadcast::channel(16);
    for _i in 0..max_packets {
        // let mut rx0 = tx.subscribe();
        let h = tokio::spawn(async move{
            // let mut buf = bytes::BytesMut::with_capacity(packet_size);
            // let pkt = Packet{id:"001".to_string()};
            // bytes::BufMut::put_slice(&mut buf, pkt.id.as_bytes());
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            //let _ = rx0.recv().await;
        });
        // v.push(h);
    }
    
    println!("  spawned tasks {}, press Enter to continue", max_packets);
    let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();
    
    // let _ = tx.send(1);
    // for h in v {
    //     let _ = h.await;
    // }
    tokio::time::sleep(tokio::time::Duration::from_secs(6)).await;
}

// fn only_bu() {
//     let packet_size = 1000 as usize;
//     let max_packets = 1000*1000 as usize;
//     let mut v= Vec::new();
//     for _i in 0..max_packets {
//         v.push(vec![0i128;packet_size]);
//     }
//     drop(v)
// }


#[tokio::main]
async fn main() {
    println!("before burning, press Enter to continue");
    let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();

    burn_memory().await;

    println!("after burning, press Enter to continue");
    let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();
}

