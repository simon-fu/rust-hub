use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use futures::task::AtomicWaker;
use sysinfo::ProcessExt;
use sysinfo::SystemExt;
use tracing::info;
use core::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::sync::atomic::Ordering;
use std::thread;

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

    const MAX_TASKS:u64 = 2_000_000;

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
    
    test_mem_size().await;
    test_future().await;
    test_poll_try().await;
    let r = test_poll_try;
    r().await;

    
}
