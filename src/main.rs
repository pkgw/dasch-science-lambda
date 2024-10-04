//! The AWS/Lambda-powered DASCH data services
//!
//! It was hard to find good examples of how a Rust Lambda implementation should look. Here's
//! one good one:
//!
//! https://github.com/awslabs/aws-sdk-rust/tree/main/examples/cross_service/photo_asset_management
//!
//! Streaming lambdas are more expensive than buffered lambdas, which have a 6
//! MB response limit. So we should buffer when possible.
//!
//! Annoyingly, the buffered response mechanism can *only* output JSON, so we
//! can't emit CSV.

use lambda_runtime::{service_fn, Error};

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

    // The way that we set up our container, this is always `bootstrap`:
    let handler = std::env::var("_HANDLER").expect("_HANDLER provided");
    println!("_HANDLER: {handler}");

    let client = aws_sdk_dynamodb::Client::new(&config);
    let bin64 = gscbin::GscBinning::new64();
    let func = service_fn(|event| querycat::handle_querycat(event, &client, &bin64));
    lambda_runtime::run(func).await?;
    Ok(())
}
