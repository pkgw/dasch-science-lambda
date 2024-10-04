//! DASCH data services
//!
//! Streaming lambdas are more expensive than buffered lambdas, which have a 6
//! MB response limit. So we should buffer when possible.
//!
//! Annoyingly, the buffered response mechanism can *only* output JSON, so we
//! can't emit CSV.

use lambda_runtime::{service_fn, Error, LambdaEvent};

mod fitsfile;
mod gscbin;
mod querycat;
mod refnums;
mod s3buffer;
mod s3fits;
mod wcs;

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
) -> Result<Vec<String>, Error> {
    querycat::handle_querycat(event, dc, binning).await
}
