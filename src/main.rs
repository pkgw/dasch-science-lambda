//! DASCH data services
//!
//! Streaming lambdas are more expensive than buffered lambdas, which have a 6
//! MB response limit. So we should buffer when possible.
//!
//! Annoyingly, the buffered response mechanism can *only* output JSON, so we
//! can't emit CSV.

use lambda_runtime::{service_fn, Error, LambdaEvent};
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
) -> Result<Vec<String>, Error> {
    querycat::handle_querycat(event, dc, binning).await
}
