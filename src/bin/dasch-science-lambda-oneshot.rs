//! "Oneshot" version of the DASCH science Lambda implementations.
//!
//! This executable runs one API function, based on arguments given on the
//! command line.

use lambda_runtime::Error;
use serde_json::Value;
use std::env;

use dasch_science_lambda::Services;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut args = env::args();
    args.next(); // skip argv[0]

    let arn = args.next().ok_or_else(|| -> Error {
        "first argument should be ARN to use (cutout, querycat, queryexps)".into()
    })?;

    let json_text = args
        .next()
        .ok_or_else(|| -> Error { "second argument should be JSON payload text".into() })?;
    let payload: Value = serde_json::from_str(&json_text)?;

    let svcs = Services::init().await?;
    let result = svcs.dispatch(arn, Some(payload)).await?;

    serde_json::to_writer(std::io::stdout().lock(), &result)?;
    Ok(())
}
