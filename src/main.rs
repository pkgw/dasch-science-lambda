//! The AWS/Lambda-powered DASCH data services
//!
//! It was hard to find good examples of how a Rust Lambda implementation should look. Here's
//! one good one:
//!
//! <https://github.com/awslabs/aws-sdk-rust/tree/main/examples/cross_service/photo_asset_management>
//!
//! Streaming lambdas are more expensive than buffered lambdas, which have a 6
//! MB response limit. So we should buffer when possible.
//!
//! Annoyingly, the buffered response mechanism can *only* output JSON, so we
//! can't emit CSV.

use lambda_runtime::{service_fn, tracing, Error, LambdaEvent};
use serde_json::Value;

mod cutout;
mod fitsfile;
mod gscbin;
mod querycat;
mod queryexps;
mod refnums;
mod s3buffer;
mod s3fits;
mod wcs;

pub const ENVIRONMENT: &str = "dev";

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_target(false) // don't print the module name
        .without_time() // don't print time (CloudWatch has it)
        .init();

    let config = aws_config::load_from_env().await;

    // The way that we set up our container, this is always `bootstrap`:
    let handler = std::env::var("_HANDLER").expect("_HANDLER provided");
    println!("_HANDLER: {handler}");

    s3fits::register(config.clone());

    let dc = aws_sdk_dynamodb::Client::new(&config);
    let s3c = aws_sdk_s3::Client::new(&config);
    let bin1 = gscbin::GscBinning::new1();
    let bin64 = gscbin::GscBinning::new64();

    let func = service_fn(|event: LambdaEvent<Value>| async {
        dispatcher(event, &dc, &s3c, &bin1, &bin64).await
    });

    lambda_runtime::run(func).await?;
    Ok(())
}

async fn dispatcher(
    event: LambdaEvent<Value>,
    dc: &aws_sdk_dynamodb::Client,
    s3c: &aws_sdk_s3::Client,
    bin1: &gscbin::GscBinning,
    bin64: &gscbin::GscBinning,
) -> Result<Value, Error> {
    let (request, context) = event.into_parts();
    let cfg = context.env_config;
    println!(
        "*** fn name={} version={} {:?}",
        cfg.function_name, cfg.version, request
    );

    if cfg.function_name.ends_with("cutout") {
        Ok(cutout::handler(request, &dc).await?)
    } else if cfg.function_name.ends_with("querycat") {
        Ok(querycat::handler(request, &dc, &bin64).await?)
    } else if cfg.function_name.ends_with("queryexps") {
        Ok(queryexps::handler(request, &dc, &s3c, &bin1).await?)
    } else {
        Err(format!(
            "unhandled function name={} version={}",
            cfg.function_name, cfg.version
        )
        .into())
    }
}
