//! Re-exports of basic HTTP types, and `HttpExposedError` and associated
//! helpers.
//!
//! This module only exists because our dependencies define a million different
//! "response" and "body" types, and there are only specific ones that work for
//! us.

use serde_json::json;
use std::{
    error::Error,
    fmt::{Display, Error as FmtError, Formatter},
};

pub use aws_lambda_events::encodings::Body;

pub type Response = lambda_http::Response<Body>;

pub use http::response::Builder as ResponseBuilder;

pub use http::StatusCode;

/// A class for errors that should be returned as specific HTTP error codes.
///
/// By default, any errors that bubble up through our processing chain will
/// result in an HTTP 500 "Internal Server Error" result; they probably also
/// cause the Lambda handler to abort. But sometimes we encounter errors that
/// have understood causes that should be reported to the caller. Use this class
/// to express them. The high-level dispatcher function will notice these and
/// turn them into proper HTTP responses with a simple JSON payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpExposedError {
    status_code: StatusCode,
    message: String,
}

impl Display for HttpExposedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        write!(f, "{} (HTTP status {})", self.message, self.status_code)
    }
}

impl Error for HttpExposedError {}

// The implementation for Box<> here is convenience for the usage of this
// method in the high-level dispatch funciton.
impl TryInto<Response> for Box<HttpExposedError> {
    type Error = lambda_runtime::Error;

    fn try_into(self) -> Result<Response, lambda_runtime::Error> {
        let builder = ResponseBuilder::new().status(self.status_code);
        let text = serde_json::to_string(
            &json!({"status_code": self.status_code.as_u16(), "error": self.message}),
        )?;
        Ok(builder.body(Body::Text(text))?)
    }
}

impl HttpExposedError {
    /// Construct a new `HttpExposedError`.
    pub fn new<S: ToString>(status_code: StatusCode, message: S) -> Self {
        HttpExposedError {
            status_code,
            message: message.to_string(),
        }
    }

    /// Generate a `Result::Err` containing a `lambda_runtime::Error` that wraps
    /// an `HttpExposedError` with the specified message an an HTTP 400 Bad
    /// Request status code.
    pub fn bad_request<T, S: ToString>(message: S) -> Result<T, lambda_runtime::Error> {
        Err(HttpExposedError::new(StatusCode::BAD_REQUEST, message).into())
    }

    /// Generate a `Result::Err` containing a `lambda_runtime::Error` that wraps
    /// an `HttpExposedError` with the specified message an an HTTP 404 Not
    /// Found status code.
    pub fn not_found<T, S: ToString>(message: S) -> Result<T, lambda_runtime::Error> {
        Err(HttpExposedError::new(StatusCode::NOT_FOUND, message).into())
    }

    /// Generate a `Result::Err` containing a `lambda_runtime::Error` that wraps
    /// an `HttpExposedError` with the specified message an an HTTP 422
    /// Unprocessable Content status code.
    ///
    /// (There seems to be some disagreement as to whether this code is
    /// textualized as "Unprocessable Entity" or "Unprocessable Content".)
    pub fn unprocessable_content<T, S: ToString>(message: S) -> Result<T, lambda_runtime::Error> {
        Err(HttpExposedError::new(StatusCode::UNPROCESSABLE_ENTITY, message).into())
    }
}

/// An extension trait for Option that helps with turning Nones into various
/// kinds of HTTP errors.
pub trait HttpOptionExt<T> {
    /// If the option is `Some(v)`, produce `Ok(v)`; otherwise produce a
    /// `lambda_runtime::Error` downcastable to an `HttpExposedError` with a
    /// status code of HTTP 400 Bad Request, and the associated message.
    fn ok_or_bad_request<S: ToString>(self, message: S) -> Result<T, lambda_runtime::Error>;

    /// If the option is `Some(v)`, produce `Ok(v)`; otherwise produce a
    /// `lambda_runtime::Error` downcastable to an `HttpExposedError` with a
    /// status code of HTTP 404 Not Found, and the associated message.
    fn ok_or_not_found<S: ToString>(self, message: S) -> Result<T, lambda_runtime::Error>;

    /// If the option is `Some(v)`, produce `Ok(v)`; otherwise produce a
    /// `lambda_runtime::Error` downcastable to an `HttpExposedError` with a
    /// status code of HTTP 422 Unprocessable Content, and the associated message.
    fn ok_or_unprocessable<S: ToString>(self, message: S) -> Result<T, lambda_runtime::Error>;
}

impl<T> HttpOptionExt<T> for Option<T> {
    fn ok_or_bad_request<S: ToString>(self, message: S) -> Result<T, lambda_runtime::Error> {
        match self {
            Some(v) => Ok(v),
            None => HttpExposedError::bad_request(message),
        }
    }

    fn ok_or_not_found<S: ToString>(self, message: S) -> Result<T, lambda_runtime::Error> {
        match self {
            Some(v) => Ok(v),
            None => HttpExposedError::not_found(message),
        }
    }

    fn ok_or_unprocessable<S: ToString>(self, message: S) -> Result<T, lambda_runtime::Error> {
        match self {
            Some(v) => Ok(v),
            None => HttpExposedError::unprocessable_content(message),
        }
    }
}
