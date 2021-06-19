




// refer https://doc.rust-lang.org/reference/conditional-compilation.html?highlight=target_os#target_os
// refer https://doc.rust-lang.org/rust-by-example/attribute/cfg.html
// refer https://cloud.tencent.com/developer/article/1138651
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


fn print_malloc_info(){
    #[cfg(target_os = "linux")]
    {
        use libc::mallinfo;
        let info = unsafe{mallinfo()};
        println!("mallinfo: arena {}", info.arena);
        println!("mallinfo: ordblks {}", info.ordblks);
        println!("mallinfo: smblks {}", info.smblks);
        println!("mallinfo: hblks {}", info.hblks);
        println!("mallinfo: hblkhd {}", info.hblkhd);
        println!("mallinfo: usmblks {}", info.usmblks);
        println!("mallinfo: fsmblks {}", info.fsmblks);
        println!("mallinfo: uordblks {}", info.uordblks);
        println!("mallinfo: fordblks {}", info.fordblks);
        println!("mallinfo: keepcost {}", info.keepcost);
    }
    
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
    println!("MALLOC_TRIM_THRESHOLD_=[{:?}]", std::env::var_os("MALLOC_TRIM_THRESHOLD_"));
    println!("MALLOC_TOP_PAD_=[{:?}]", std::env::var_os("MALLOC_TOP_PAD_"));
    print_malloc_info();
    
    println!("burning type: [tokio task sleep]");

    println!("before burning, press Enter to continue");
    let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();

    burn_memory().await;

    #[cfg(target_os = "linux")]
    {
        print_malloc_info();
        println!("after burning, press Enter to call malloc_trim");
        let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();
        call_malloc_trim();

        println!("after malloc_trim, press Enter to print_malloc_info");
        let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();
        print_malloc_info();
    }

    println!("final, press Enter to exit");
    let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8]).unwrap();
}


