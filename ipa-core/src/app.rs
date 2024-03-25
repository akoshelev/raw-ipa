use std::future::Future;
use std::sync::Mutex;
use async_trait::async_trait;
use crate::{
    helpers::{
        query::{QueryConfig, QueryInput},
        Transport, TransportImpl,
    },
    hpke::{KeyPair, KeyRegistry},
    protocol::QueryId,
    query::{
        NewQueryError, QueryCompletionError, QueryInputError, QueryProcessor, QueryStatus,
        QueryStatusError,
    },
    sync::Arc,
};
use crate::helpers::{ApiError, BodyStream, HelperIdentity, HelperResponse, RequestHandler, TransportIdentity};
use crate::helpers::query::PrepareQuery;
use crate::helpers::routing::{Addr, RouteId};

pub struct Setup {
    query_processor: Arc<QueryProcessor>,
    handler_setup: RequestHandlerSetup,
}

/// The API layer to interact with a helper.
#[must_use]
#[derive(Clone)]
pub struct HelperApp {
    query_processor: Arc<QueryProcessor>,
    transport: TransportImpl,
}

pub struct QueryRequestHandler {
    qp: Arc<QueryProcessor>,
    transport: Arc<Mutex<Option<TransportImpl>>>
}

#[async_trait]
impl RequestHandler for QueryRequestHandler {
    type Identity = HelperIdentity;

    async fn handle(&self, req: Addr<Self::Identity>, data: BodyStream) -> Result<HelperResponse, ApiError> {
        fn ext_query_id(req: &Addr<HelperIdentity>) -> Result<QueryId, ApiError> {
            req.query_id.ok_or_else(|| ApiError::BadRequest("Query input is missing query_id argument".into()))
        }

        let qp = Arc::clone(&self.qp);

        Ok(match req.route {
            RouteId::Records => {
                // TODO: return failure as handlers are not supposed to handle these
                HelperResponse::ok()
            }
            RouteId::ReceiveQuery => {
                let req = req.into::<QueryConfig>()?;
                let transport = self.transport.lock().unwrap().as_ref().unwrap().clone();
                HelperResponse::from(qp.new_query(transport, req).await?)
            }
            RouteId::PrepareQuery => {
                let req = req.into::<PrepareQuery>()?;
                let transport = self.transport.lock().unwrap().as_ref().unwrap().clone();
                HelperResponse::from(qp.prepare(&transport, req)?)
            }
            RouteId::QueryInput => {
                let query_id = ext_query_id(&req)?;
                let transport = self.transport.lock().unwrap().as_ref().unwrap().clone();
                HelperResponse::from(qp.receive_inputs(transport, QueryInput {
                    query_id,
                    input_stream: data,
                })?)
            }
            RouteId::QueryStatus => {
                let query_id = ext_query_id(&req)?;
                HelperResponse::from(qp.query_status(query_id)?)
            }
            RouteId::CompleteQuery => {
                let query_id = ext_query_id(&req)?;
                HelperResponse::from(qp.complete(query_id).await?)
            }
        })
    }
}

#[derive(Clone)]
pub struct RequestHandlerSetup {
    qp: Arc<QueryProcessor>,
    transport_container: Arc<Mutex<Option<TransportImpl>>>,
}

impl RequestHandlerSetup {
    fn new(qp: Arc<QueryProcessor>) -> Self {
        Self {
            qp,
            transport_container: Arc::new(Mutex::new(None)),
        }
    }

    pub fn make_handler(&self) -> QueryRequestHandler {
        QueryRequestHandler {
            qp: Arc::clone(&self.qp),
            transport: Arc::clone(&self.transport_container),
        }
    }

    fn finish(self, transport: TransportImpl) {
        let mut guard = self.transport_container.lock().unwrap();
        *guard = Some(transport);
    }
}


impl Setup {
    #[must_use]
    pub fn new() -> (Self, RequestHandlerSetup) {
        Self::with_key_registry(KeyRegistry::empty())
    }

    #[must_use]
    pub fn with_key_registry(
        key_registry: KeyRegistry<KeyPair>,
    ) -> (Self, RequestHandlerSetup) {
        let query_processor = Arc::new(QueryProcessor::new(key_registry));
        let handler_setup = RequestHandlerSetup::new(Arc::clone(&query_processor));
        let this = Self {
            query_processor,
            handler_setup: handler_setup.clone(),
        };

        // TODO: weak reference to query processor to prevent mem leak
        (this, handler_setup)
    }

    /// Instantiate [`HelperApp`] by connecting it to the provided transport implementation
    pub fn connect(self, transport: TransportImpl) -> HelperApp {
        self.handler_setup.finish(transport.clone());
        HelperApp::new(transport, self.query_processor)
    }
}

impl HelperApp {
    pub fn new(transport: TransportImpl, query_processor: Arc<QueryProcessor>) -> Self {
        Self {
            query_processor,
            transport,
        }
    }

    /// Initiates a new query on this helper. In case if query is accepted, the unique [`QueryId`]
    /// identifier is returned, otherwise an error indicating what went wrong is reported back.
    ///
    /// ## Errors
    /// If query is rejected for any reason.
    pub async fn start_query(&self, query_config: QueryConfig) -> Result<QueryId, NewQueryError> {
        Ok(self
            .query_processor
            .new_query(Transport::clone_ref(&self.transport), query_config)
            .await?
            .query_id)
    }

    /// Sends query input to a helper.
    ///
    /// ## Errors
    /// Propagates errors from the helper.
    pub fn execute_query(&self, input: QueryInput) -> Result<(), ApiError> {
        let transport = <TransportImpl as Clone>::clone(&self.transport);
        self.query_processor.receive_inputs(transport, input)?;
        Ok(())
    }

    /// Retrieves the status of a query.
    ///
    /// ## Errors
    /// Propagates errors from the helper.
    pub fn query_status(&self, query_id: QueryId) -> Result<QueryStatus, ApiError> {
        Ok(self.query_processor.query_status(query_id)?)
    }

    /// Waits for a query to complete and returns the result.
    ///
    /// ## Errors
    /// Propagates errors from the helper.
    pub async fn complete_query(&self, query_id: QueryId) -> Result<Vec<u8>, ApiError> {
        Ok(self.query_processor.complete(query_id).await?.as_bytes())
    }
}
