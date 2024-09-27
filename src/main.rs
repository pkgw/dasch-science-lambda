use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;

mod gscbin;
mod querycat;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_dynamodb::Client::new(&config);
    let bin64 = gscbin::GscBinning::new64();

    let func = service_fn(|event| handler(event, &client, &bin64));
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn handler(
    event: LambdaEvent<querycat::Request>,
    dc: &aws_sdk_dynamodb::Client,
    binning: &gscbin::GscBinning,
) -> Result<Value, Error> {
    querycat::handle_querycat(event, dc, binning).await
}
