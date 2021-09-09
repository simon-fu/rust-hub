
use anyhow::Result;
use tracing::Level;


mod verify_origin;
mod single_mem_node;
mod zraft;


#[tokio::main]
async fn main() -> Result<()>{
    tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(Level::DEBUG)
        .init();

    
    // single_mem_node::run_main();
    verify_origin::run_main().await
    
}

