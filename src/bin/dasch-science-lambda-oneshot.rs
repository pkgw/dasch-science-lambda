//! "Oneshot" version of the DASCH science Lambda implementations.
//!
//! This executable runs one API function, based on arguments given on the
//! command line.

use lambda_runtime::Error;
use serde_json::Value;
use std::env;

use dasch_science_lambda::{
    http_types::{Body, StatusCode},
    Services,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut args = env::args();
    args.next(); // skip argv[0]

    let arn = args.next().ok_or_else(|| -> Error {
        "first argument should be ARN to use (cutout, lightcurve, etc.)".into()
    })?;

    let json_text = args
        .next()
        .ok_or_else(|| -> Error { "second argument should be JSON payload text".into() })?;
    let payload: Value = serde_json::from_str(&json_text)?;

    let svcs = Services::init().await?;
    let result = svcs.dispatch(arn, Some(payload)).await?;

    // Now for some extremely lame textualization of the response. Nothing
    // should go to stdout besides the response body, to support daschlab's
    // local-program API mode.

    let s = result.status();

    if s != StatusCode::OK {
        eprintln!("status code: {}", s);
    }

    for (hname, hvalue) in result.headers() {
        eprintln!(
            "header: {} = {}",
            hname,
            hvalue.to_str().unwrap_or("(not ASCII)")
        );
    }

    match result.body() {
        Body::Empty => {}
        Body::Text(s) => print!("{}", s),
        Body::Binary(b) => {
            // We could easily dump actual data here ...
            print!("(binary body of {} bytes)", b.len())
        }
    }

    Ok(())
}
