use tokio::io;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    println!("Hello from Indistore!");
    Ok(())
}