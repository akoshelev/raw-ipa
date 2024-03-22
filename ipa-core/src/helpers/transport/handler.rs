use std::any::type_name;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::iter::once;
use std::marker::PhantomData;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::stream;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use crate::error::BoxError;
use crate::helpers::transport::routing::Addr;
use crate::helpers::{BodyStream, BytesStream, HelperIdentity, TransportIdentity};
use crate::helpers::query::PrepareQuery;
use crate::helpers::transport::stream::BoxBytesStream;
use crate::protocol::QueryId;
use crate::query::{NewQueryError, PrepareQueryError, ProtocolResult, QueryCompletionError, QueryInputError, QueryStatus, QueryStatusError};


pub struct HelperResponse {
    body: Vec<u8>
    // Empty,
    // QueryCreated(QueryId),
    // QueryStatus(QueryStatus),
}

impl Debug for HelperResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl HelperResponse {
    pub fn ok() -> Self {
        Self {
            body: Vec::new()
        }
    }

    pub fn into_body(self) -> Vec<u8> {
        self.body
    }

    pub fn into_owned<T: DeserializeOwned>(self) -> T {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| panic!("Failed to deserialize {:?} into {}: {e}", &self.body, type_name::<T>()))
    }
}

impl From<PrepareQuery> for HelperResponse {
    fn from(value: PrepareQuery) -> Self {
        let v = serde_json::to_vec(&json!({"query_id": value.query_id})).unwrap();
        Self {
            body: v
        }
    }
}

impl From<()> for HelperResponse {
    fn from(value: ()) -> Self {
        Self::ok()
    }
}

impl From<QueryStatus> for HelperResponse {
    fn from(value: QueryStatus) -> Self {
        let v = serde_json::to_vec(&value).unwrap();
        Self {
            body: v
        }
    }
}

impl <R: AsRef<dyn ProtocolResult>> From<R> for HelperResponse {
    fn from(value: R) -> Self {
        let v = value.as_ref().as_bytes();
        Self {
            body: v
        }
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
pub trait RequestHandler : Send + Sync + 'static {
    type Identity: TransportIdentity;
    async fn handle(&self, req: Addr<Self::Identity>, data: BodyStream) -> Result<HelperResponse, Error>;
}

pub struct PanickingHandler<I: TransportIdentity> {
    phantom: PhantomData<I>
}

impl <I: TransportIdentity> Default for PanickingHandler<I> {
    fn default() -> Self {
        Self {
            phantom: PhantomData
        }
    }
}

#[async_trait]
impl <I: TransportIdentity> RequestHandler for PanickingHandler<I> {
    type Identity = I;

    async fn handle(&self, req: Addr<Self::Identity>, data: BodyStream) -> Result<HelperResponse, Error> {
        panic!("unexpected call: {req:?}");
    }
}