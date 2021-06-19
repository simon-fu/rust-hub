
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

// refer https://doc.rust-lang.org/reference/conditional-compilation.html?highlight=target_os#target_os
// refer https://doc.rust-lang.org/rust-by-example/attribute/cfg.html
#[cfg(target_os = "linux")]
fn call_malloc_trim() {
    extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }
    let freed = unsafe { 
        malloc_trim(128*1024)
    };
    println!("malloc_trim freed {}", freed);
}


async fn burn_memory(){
    println!("burning ...");

    let max_tasks = 1*1000*1000 as usize;

    for _i in 0..max_tasks {
        tokio::spawn(async move{
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        });
    }
    
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
}


#[tokio::main]
async fn main() {
    println!("burning type: [tokio task sleep and jemallocator]");

    println!("before burning, press Enter to continue");
    let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();

    burn_memory().await;
    
    #[cfg(target_os = "linux")]
    {
        println!("after burning, press Enter to call malloc_trim");
        let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();
        call_malloc_trim();
    }

    println!("after burning, press Enter to continue");
    let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();
}

