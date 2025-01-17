
#[tokio::main]
async fn main() {
    use qust_api::thx::market::test_market;
    test_market().await;
}

