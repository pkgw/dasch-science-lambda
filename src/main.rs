use lambda_runtime::{
    service_fn,
    streaming::{Body, Response},
    Error, LambdaEvent,
};
use std::sync::Arc;

mod gscbin;
mod querycat;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = aws_config::load_from_env().await;
    let client = Arc::new(aws_sdk_dynamodb::Client::new(&config));
    let bin64 = Arc::new(gscbin::GscBinning::new64());
    let func = service_fn(|event| handler(event, client.clone(), bin64.clone()));
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn handler(
    event: LambdaEvent<querycat::Request>,
    dc: Arc<aws_sdk_dynamodb::Client>,
    binning: Arc<gscbin::GscBinning>,
) -> Result<Response<Body>, Error> {
    querycat::handle_querycat(event, dc, binning).await
}
