//! Re-exports of basic HTTP types
//!
//! This module only exists because our dependencies define a million different
//! "response" and "body" types, and there are only specific ones that work for us.

pub use aws_lambda_events::encodings::Body;

pub type Response = lambda_http::Response<Body>;

pub use http::response::Builder as ResponseBuilder;

pub use http::StatusCode;
