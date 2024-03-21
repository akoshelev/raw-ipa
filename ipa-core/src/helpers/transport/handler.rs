use std::future::Future;
use async_trait::async_trait;
use crate::error::BoxError;
use crate::helpers::transport::routing::Addr;
use crate::helpers::{BodyStream, BytesStream, HelperIdentity, TransportIdentity};
use crate::helpers::query::PrepareQuery;
use crate::helpers::transport::stream::BoxBytesStream;
use crate::query::{NewQueryError, PrepareQueryError, ProtocolResult, QueryCompletionError, QueryInputError, QueryStatus, QueryStatusError};


#[derive(Debug)]
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

/// There is a limitation for RPITIT that traits can't be made object-safe, hence the use of async_trait
#[async_trait]
pub trait RequestHandler : Send {
    type Identity: TransportIdentity;
    async fn handle(&self, req: Addr<Self::Identity>, data: BodyStream) -> Result<HelperResponse, Error>;
}