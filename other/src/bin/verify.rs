use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use futures::task::AtomicWaker;
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
    

    test_future().await;
}
