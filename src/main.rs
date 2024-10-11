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

use lambda_runtime::{service_fn, Error, LambdaEvent};

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
    let config = aws_config::load_from_env().await;

    // The way that we set up our container, this is always `bootstrap`:
    let handler = std::env::var("_HANDLER").expect("_HANDLER provided");
    println!("_HANDLER: {handler}");

    // TODO: once-cell this:
    s3fits::register(config.clone());

    // TEMPORARY: hardcoding queryexps service
    let dc = aws_sdk_dynamodb::Client::new(&config);
    let s3c = aws_sdk_s3::Client::new(&config);
    let bin1 = gscbin::GscBinning::new1();
    let func = service_fn(|event: LambdaEvent<queryexps::Request>| {
        let (request, context) = event.into_parts();
        let cfg = context.env_config;
        println!("*** fn name={} version={}", cfg.function_name, cfg.version);
        queryexps::handle_queryexps(request, &dc, &s3c, &bin1)
    });
    lambda_runtime::run(func).await?;

    Ok(())
}
