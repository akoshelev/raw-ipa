use std::future::Future;
use crate::error::BoxError;
use crate::helpers::transport::routing::Addr;
use crate::helpers::{BodyStream, HelperIdentity, TransportIdentity};
use crate::helpers::query::PrepareQuery;
use crate::query::{NewQueryError, PrepareQueryError, ProtocolResult, QueryCompletionError, QueryInputError, QueryStatus, QueryStatusError};


pub struct HelperResponse;

impl HelperResponse {
    pub fn ok() -> Self {
        todo!()
    }
}

impl From<PrepareQuery> for HelperResponse {
    fn from(value: PrepareQuery) -> Self {
        todo!()
    }
}

impl From<()> for HelperResponse {
    fn from(value: ()) -> Self {
        todo!()
    }
}

impl From<QueryStatus> for HelperResponse {
    fn from(value: QueryStatus) -> Self {
        todo!()
    }
}

impl <R: AsRef<dyn ProtocolResult>> From<R> for HelperResponse {
    fn from(value: R) -> Self {
        todo!()
    }
}


/// Union of error types returned by API operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    NewQuery(#[from] NewQueryError),
    #[error(transparent)]
    QueryInput(#[from] QueryInputError),
    #[error(transparent)]
    QueryPrepare(#[from] PrepareQueryError),
    #[error(transparent)]
    QueryCompletion(#[from] QueryCompletionError),
    #[error(transparent)]
    QueryStatus(#[from] QueryStatusError),
    #[error(transparent)]
    DeserializationFailure(#[from] serde_json::Error),
    #[error("MalformedRequest: {0}")]
    BadRequest(BoxError)
}

pub trait RequestHandler {
    fn handle(&self, req: Addr<HelperIdentity>, data: BodyStream) -> impl Future<Output = Result<HelperResponse, Error>>;
}