#![allow(unused)]
use std::time::Duration;
use std::mem;
use tokio::time;
use tokio::time::{Instant, Sleep};

const ONE_MS: Duration = Duration::from_millis(1);

async fn async_fn_blank() {

}

async fn async_fn_blank1() {
    async_fn_blank().await;
}

async fn async_fn_blank2() {
    async_fn_blank1().await;
}


async fn async_fn_sleep() {
    time::sleep(ONE_MS).await;
}

async fn async_fn_wrapping_sleep() {
    async_fn_sleep().await;
}

async fn async_fn_wrapping_wrapping_sleep() {
    async_fn_wrapping_sleep().await;
}

async fn async_fn_boxed_sleep() {
    Box::pin(tokio::time::sleep(ONE_MS)).await;
}

async fn async_fn_wrapping_boxed_sleep() {
    async_fn_boxed_sleep().await;
}

async fn async_fn_wrapping_wrapping_boxed_sleep() {
    async_fn_wrapping_boxed_sleep().await;
}

async fn async_fn_aligned_structure0() {
    // #[repr(C, align(128))]
    struct SoBigAField([u8; 128]);
    struct FakeSleep(SoBigAField, [u8; 512]);
    let mut refered = FakeSleep(SoBigAField([0u8; 128]), [0u8; 512]);
    async_fn_blank().await;
    refered.1[0] = 1;
}

async fn async_fn_aligned_structure1() {
    #[repr(C, align(128))]
    struct SoBigAField([u8; 128]);
    struct FakeSleep(SoBigAField, [u8; 512]);
    let mut refered = FakeSleep(SoBigAField([0u8; 128]), [0u8; 512]);
    async_fn_blank().await;
    refered.1[0] = 1;
}

async fn async_fn_wrapping_aligned_structure() {
    async_fn_aligned_structure0().await;
}

async fn async_fn_wrapping_wrapping_aligned_structure() {
    async_fn_wrapping_aligned_structure().await;
}

macro_rules! print_futures_size {
    ($type: ty) => {
            println!("{:48} : {}", stringify!($type), mem::size_of::<$type>());
    };
    ($($gen_future: expr),*) => {
        $(
            println!("{:48} : {}", stringify!($gen_future), mem::size_of_val(&$gen_future));
        )*
    };
}

#[tokio::main]
async fn main() {
    // Sleep: https://github.com/tokio-rs/tokio/blob/f3ed064a269fd72711e40121dad1a9fd9f16bdc0/tokio/src/time/driver/sleep.rs#L164
    // TimerEntry: https://github.com/tokio-rs/tokio/blob/f3ed064a269fd72711e40121dad1a9fd9f16bdc0/tokio/src/time/driver/entry.rs#L284
    // TimerShared: https://github.com/tokio-rs/tokio/blob/f3ed064a269fd72711e40121dad1a9fd9f16bdc0/tokio/src/time/driver/entry.rs#L327
    // StateCell: https://github.com/tokio-rs/tokio/blob/f3ed064a269fd72711e40121dad1a9fd9f16bdc0/tokio/src/time/driver/entry.rs#L96
    // CachePadded: https://github.com/tokio-rs/tokio/blob/b42f21ec3e212ace25331d0c13889a45769e6006/tokio/src/util/pad.rs#L11
    print_futures_size![Sleep];
    print_futures_size!(
        async_fn_blank(), 
        async_fn_blank1(), 
        async_fn_blank2(), 
        time::sleep(ONE_MS), 
        async_fn_sleep(), 
        async_fn_wrapping_sleep(), 
        async_fn_wrapping_wrapping_sleep(), 
        async_fn_boxed_sleep(), 
        async_fn_wrapping_boxed_sleep(), 
        async_fn_wrapping_wrapping_boxed_sleep(), 
        async_fn_aligned_structure0(), 
        async_fn_aligned_structure1(), 
        async_fn_wrapping_aligned_structure(), 
        async_fn_wrapping_wrapping_aligned_structure());
}
