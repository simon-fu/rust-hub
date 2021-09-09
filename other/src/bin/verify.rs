use std::any::Any;
use std::num::NonZeroI64;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;
use futures::FutureExt;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use futures::task::AtomicWaker;
use sysinfo::ProcessExt;
use sysinfo::SystemExt;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time;
use tracing::info;
use core::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::sync::atomic::Ordering;


struct TimerFuture {
    shared_state: Arc<SharedState>,
}

/// Future和Thread共享的数据
struct SharedState {
    completed: AtomicBool,
    waker: futures::task::AtomicWaker,
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 调用register更新Waker，再读取共享的completed变量.
        self.shared_state.waker.register(cx.waker());	
        if self.shared_state.completed.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl TimerFuture {
    pub fn new(duration: Duration) -> Self {
        let shared_state = Arc::new(SharedState {
            completed: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        });

        let thread_shared_state = shared_state.clone();
        thread::spawn(move || {
            thread::sleep(duration);
            thread_shared_state.completed.store(true, Ordering::SeqCst);
            thread_shared_state.waker.wake();
        });

        TimerFuture { shared_state }
    }
}
async fn test_future() {
    let timer = TimerFuture::new(Duration::from_millis(2000));
    info!("wait...");
    timer.await;
    info!("waiting done");
}

async fn test_poll_try(){
    // use core::task::Poll;
    let f1 = async {
        let r = Ok::<u64, String>(0);
        let r = Poll::Ready(r);
        let _r = r?;
        Ok::<u64, String>(0)
    };

    let f2 = async {
        let r = Ok::<u64, String>(0);
        let r = Poll::Ready(r);
        let _r = futures::ready!(r);
        Poll::Ready(0u64)
    };
    drop(f1);
    drop(f2);
}

async fn async_fn0(){

}

async fn async_fn1(){
    println!("  async_fn1, val is {}", 1);
}

async fn async_fn2(){
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
}

async fn async_fn3(){
    let d = std::time::Duration::from_millis(1);
    tokio::time::sleep(d).await;
}

async fn async_fn4(){
    async_fn3().await;
}

async fn async_fn5(){
    async_fn4().await;
}

async fn async_fn6(){
    async_fn5().await;
}

async fn async_fn7(){
    let v = [1u64; 6];
    for n in v.iter() {
        tokio::time::sleep(Duration::from_millis(*n)).await;
    }
}

async fn print_future_size(name: &str, f: impl Future<Output = ()>) {
    println!("{} size {}", name, std::mem::size_of_val(&f));
    // f.await;
}

fn make_future() -> impl Future<Output = ()> {
    let fut = async move {
        tokio::time::sleep(Duration::from_secs(99999999)).await;
    };
    fut
}

async fn test_mem_size() {
    let a1 = [1u64; 1];
    let a2 = [1u64; 5];
    let a3 = [1u64; 6];
    println!("a1 size {}", std::mem::size_of_val(&a1));
    println!("a2 size {}", std::mem::size_of_val(&a2));
    println!("a3 size {}", std::mem::size_of_val(&a3));

    let v1 = vec![1u64; 1];
    let v2 = vec![1u64; 5];
    let v3 = vec![1u64; 6];
    println!("v1 size {}", std::mem::size_of_val(&v1));
    println!("v2 size {}", std::mem::size_of_val(&v2));
    println!("v3 size {}", std::mem::size_of_val(&v3));

    print_future_size("async_fn0", async_fn0()).await;
    print_future_size("async_fn1", async_fn1()).await;
    print_future_size("async_fn2", async_fn2()).await;
    print_future_size("async_fn3", async_fn3()).await;
    print_future_size("async_fn4", async_fn4()).await;
    print_future_size("async_fn5", async_fn5()).await;
    print_future_size("async_fn6", async_fn6()).await;
    print_future_size("async_fn7", async_fn7()).await;

    const MAX_TASKS:u64 = 1_000_000;

    let fut = make_future();
    let fut_size = std::mem::size_of_val(&fut);

    let pid = sysinfo::get_current_pid().unwrap();
    let mut system = sysinfo::System::new_all();
    system.refresh_all();
    
    let mem1;
    {
        system.refresh_all();
        let proc_ = system.processes().get(&pid).unwrap();
        println!("{} => mem {:?}, fut {}", pid, proc_.memory(), fut_size);
        mem1 = proc_.memory();
    }

    for _ in 0..MAX_TASKS {
        tokio::spawn(make_future());
    }

    {
        system.refresh_all();
        let proc_ = system.processes().get(&pid).unwrap();
        let diff = proc_.memory() - mem1;
        let task_mem = diff * 1000 / MAX_TASKS;
        println!("{} <= mem {:?}, fut {}, task {}", pid, proc_.memory(), fut_size, task_mem);
        

    }

    for _ in 0..999999{
        system.refresh_all();
        let proc_ = system.processes().get(&pid).unwrap();
        println!("{} <= mem {:?}, fut {}", pid, proc_.memory(), fut_size);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    std::process::exit(0);
}

fn overflow_fn() {
    println!("overflow_fn");
    let v = [0u8; 1*1024*1024*1024];
    println!("  v size {}", std::mem::size_of_val(&v));
}

async fn overflow_async() {
    println!("overflow_async");
    use std::io::Write;
    let _r = std::io::stdout().flush();
    let v = [0u8; 1*1024*1024*1024];
    println!("  v size {}", std::mem::size_of_val(&v));
}

const ONE_MS: Duration = Duration::from_millis(1);

macro_rules! print_futures_size {
    ($type: ty) => {
            println!("{:48} : {}", stringify!($type), std::mem::size_of::<$type>());
    };
    ($($gen_future: expr),*) => {
        $(
            println!("{:48} : {}", stringify!($gen_future), std::mem::size_of_val(&$gen_future));
        )*
    };
}

async fn fn_blank() {

}

async fn big_var_directly() {
    println!("  big_var_directly");
    let v = [0u8; 128*1024*1024];
    println!("    simple: v[0]={}", v[0]);
    println!("    simple: v[last]={}", v[v.len()-1]);
}

async fn big_var_cross_await() {
    println!("  big_var_cross_await");
    use std::io::Write;
    let _r = std::io::stdout().flush();
    let mut v = [0u8; 128*1000*1000];
    println!("    before cross await: v[0]={}", v[0]);
    println!("    before cross await: v[last]={}", v[v.len()-1]);
    time::sleep(ONE_MS).await;
    v[0] = 1;
    println!("    after cross await: v[0]={}", v[0]);
    println!("    after cross await: v[last]={}", v[v.len()-1]);
    println!("    v addr {}", std::ptr::addr_of!(v) as usize);
}


async fn wrapp_big_var_cross_await_1() {
    big_var_cross_await().await;
}

async fn wrapp_big_var_cross_await_2() {
    wrapp_big_var_cross_await_1().await;
}


async fn not_overflow() {

    
    {
        let n = 10u64;
        let fut = big_var_cross_await();
        let p1 = std::ptr::addr_of!(n) as usize;
        let p2 = std::ptr::addr_of!(fut) as usize;
        println!("not_overflow, n addr {}, fut addr {}, fut size {}, addr diff {}", p1, p2, std::mem::size_of_val(&fut), p1-p2);
        fut.await;
    }
}

async fn expect_overflow() {
    println!("expect_overflow");
    big_var_directly().await;
}



async fn test_over_flow() {
    // fn_big_local_var().await;
    // expect_overflow().await;
    let fut = Box::pin(not_overflow());
    print_futures_size!(fn_blank(), fut);
    fut.await;
    // Box::pin(not_overflow()).await;
    Box::pin(expect_overflow()).await;
    std::process::exit(0);
}


fn test_opt_size() {
    println!(
        "i8 {}, opt {}", 
        std::mem::size_of_val(&1i8),
        std::mem::size_of_val(&Some(1i8))
    );

    println!(
        "u8 {}, opt {}", 
        std::mem::size_of_val(&1u8),
        std::mem::size_of_val(&Some(1u8))
    );

    println!(
        "i16 {}, opt {}", 
        std::mem::size_of_val(&1i16),
        std::mem::size_of_val(&Some(1i16))
    );

    println!(
        "u16 {}, opt {}", 
        std::mem::size_of_val(&1u16),
        std::mem::size_of_val(&Some(1u16))
    );

    println!(
        "i32 {}, opt {}", 
        std::mem::size_of_val(&1i32),
        std::mem::size_of_val(&Some(1i32))
    );

    println!(
        "u32 {}, opt {}", 
        std::mem::size_of_val(&1u32),
        std::mem::size_of_val(&Some(1u32))
    );

    println!(
        "i64 {}, opt {}", 
        std::mem::size_of_val(&1i64),
        std::mem::size_of_val(&Some(1i64))
    );

    println!(
        "u64 {}, opt {}", 
        std::mem::size_of_val(&1u64),
        std::mem::size_of_val(&Some(1u64))
    );

    println!(
        "i64 {}, opt {}", 
        std::mem::size_of_val(&1i64),
        std::mem::size_of_val(&Some(1i64))
    );

    println!(
        "NonZeroI64 opt {}", 
        std::mem::size_of_val(&NonZeroI64::new(1))
    );

    let n = NonZeroI64::new(0);
    println!("NonZeroI64(0) {:?}, NonZeroI64(1) {:?}", NonZeroI64::new(0), NonZeroI64::new(1));
    

    std::process::exit(0);
}



async fn try_recv0<T>(rx: &mut tokio::sync::mpsc::Receiver<T>) -> Option<T> {
    
    futures::select! {
        r = rx.recv().fuse() => {
            return r;
        }
        default => {
            return None;
        }
    };

}
async fn test_try_recv() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    println!("try_recv 1 {:?}", try_recv0(&mut rx).await);
    let _r = tx.send(1u64).await;
    println!("try_recv 2 {:?}", try_recv0(&mut rx).await);
}

async fn async_num(n: u64) -> u64 {
    n
}

async fn test_futures_unordered() {
    let (tx, rx) = broadcast::channel(10);
    let mut rx1 = tx.subscribe();
    let mut rx2 = tx.subscribe();
    let r = rx1.try_recv();
    
    let mut futs = FuturesUnordered::new();
    futs.push(rx1.recv());
    futs.push(rx2.recv());

    

    let _r = tx.send(1u64);
    while !futs.is_empty() {
        //let r = futs.select_next_some().await;
        let r = futs.next().await;
        println!("{:?}", r);
    }
    
    std::process::exit(0);
}




#[tokio::main]
async fn main() {
    use tracing_subscriber::EnvFilter;
    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV).is_ok() {
        EnvFilter::from_default_env()
    } else {
        EnvFilter::new("info")
    };

    tracing_subscriber::fmt()
        .with_target(false)
        // .with_timer(ChronoLocal::default())
        .with_env_filter(env_filter)
        .init();
    info!("");

    test_over_flow().await;

    test_mem_size().await;
    

    test_futures_unordered().await;
    test_try_recv().await;
    
    test_opt_size();
    overflow_fn();
    overflow_async().await;
    let fut = Box::pin(overflow_async());
    fut.await;

    
    test_future().await;
    test_poll_try().await;
    let r = test_poll_try;
    r().await;

    
}
