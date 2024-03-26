use std::sync::{Mutex, Weak};

use async_trait::async_trait;

use crate::{
    helpers::{
        query::{PrepareQuery, QueryConfig, QueryInput},
        routing::{Addr, RouteId},
        ApiError, BodyStream, HelperIdentity, HelperResponse, RequestHandler, Transport,
        TransportImpl,
    },
    hpke::{KeyPair, KeyRegistry},
    protocol::QueryId,
    query::{NewQueryError, QueryProcessor, QueryStatus},
    sync::Arc,
};

/// The lifecycle of request handlers is somewhat complicated. First, to initialize [`Transport`],
/// an instance of [`RequestHandler`] is required upfront. To function properly, each handler must
/// have a reference to transport.
///
/// This lifecycle is managed through this struct. An empty [`Option`], protected by a mutex
/// is passed over to transport, and it is given a value later, after transport is fully initialized.
#[derive(Default)]
struct HandlerBox {
    /// There is a cyclic dependency between handlers and transport.
    /// Handlers use transports to create MPC infrastructure as response to query requests.
    /// Transport uses handler to respond to requests.
    ///
    /// To break this cycle, transport holds a weak reference to the handler and handler
    /// uses strong references to transport.
    inner: Mutex<Option<Weak<dyn RequestHandler<Identity = HelperIdentity>>>>,
}

/// This struct is passed over to [`Transport`] to initialize it.
pub struct HandlerRef {
    inner: Weak<HandlerBox>,
}

pub struct Setup {
    query_processor: QueryProcessor,
    handler: Arc<HandlerBox>,
}

/// The API layer to interact with a helper.
#[must_use]
pub struct HelperApp {
    inner: Arc<Inner>,
    _handler: Arc<HandlerBox>,
}

struct Inner {
    query_processor: QueryProcessor,
    transport: TransportImpl,
}

impl Setup {
    #[must_use]
    pub fn new() -> (Self, HandlerRef) {
        Self::with_key_registry(KeyRegistry::empty())
    }

    #[must_use]
    pub fn with_key_registry(key_registry: KeyRegistry<KeyPair>) -> (Self, HandlerRef) {
        let query_processor = QueryProcessor::new(key_registry);
        let handler = Arc::new(HandlerBox::default());
        let this = Self {
            query_processor,
            handler: handler.clone(),
        };

        // TODO: weak reference to query processor to prevent mem leak
        (
            this,
            HandlerRef {
                inner: Arc::downgrade(&handler),
            },
        )
    }

    /// Instantiate [`HelperApp`] by connecting it to the provided transport implementation
    pub fn connect(self, transport: TransportImpl) -> HelperApp {
        let app = Arc::new(Inner {
            query_processor: self.query_processor,
            transport,
        });
        self.handler.set_handler(
            Arc::downgrade(&app) as Weak<dyn RequestHandler<Identity = HelperIdentity>>
        );

        // Handler must be kept inside the app instance. When app is dropped, handler, transport and
        // query processor are destroyed.
        HelperApp {
            inner: app,
            _handler: self.handler,
        }
    }
}

impl HelperApp {
    /// Initiates a new query on this helper. In case if query is accepted, the unique [`QueryId`]
    /// identifier is returned, otherwise an error indicating what went wrong is reported back.
    ///
    /// ## Errors
    /// If query is rejected for any reason.
    pub async fn start_query(&self, query_config: QueryConfig) -> Result<QueryId, NewQueryError> {
        Ok(self
            .inner
            .query_processor
            .new_query(Transport::clone_ref(&self.inner.transport), query_config)
            .await?
            .query_id)
    }

    /// Sends query input to a helper.
    ///
    /// ## Errors
    /// Propagates errors from the helper.
    pub fn execute_query(&self, input: QueryInput) -> Result<(), ApiError> {
        let transport = <TransportImpl as Clone>::clone(&self.inner.transport);
        self.inner
            .query_processor
            .receive_inputs(transport, input)?;
        Ok(())
    }

    /// Retrieves the status of a query.
    ///
    /// ## Errors
    /// Propagates errors from the helper.
    pub fn query_status(&self, query_id: QueryId) -> Result<QueryStatus, ApiError> {
        Ok(self.inner.query_processor.query_status(query_id)?)
    }

    /// Waits for a query to complete and returns the result.
    ///
    /// ## Errors
    /// Propagates errors from the helper.
    pub async fn complete_query(&self, query_id: QueryId) -> Result<Vec<u8>, ApiError> {
        Ok(self
            .inner
            .query_processor
            .complete(query_id)
            .await?
            .as_bytes())
    }
}

#[async_trait]
impl RequestHandler for HandlerRef {
    type Identity = HelperIdentity;

    async fn handle(
        &self,
        req: Addr<Self::Identity>,
        data: BodyStream,
    ) -> Result<HelperResponse, ApiError> {
        self.inner
            .upgrade()
            .expect("Handler exists")
            .handler()
            .handle(req, data)
            .await
    }
}

impl HandlerBox {
    fn set_handler(&self, handler: Weak<dyn RequestHandler<Identity = HelperIdentity>>) {
        let mut guard = self.inner.lock().unwrap();
        assert!(guard.is_none(), "Handler can be set only once");
        *guard = Some(handler);
    }

    fn handler(&self) -> Arc<dyn RequestHandler<Identity = HelperIdentity>> {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .expect("Handler exists")
    }
}

#[async_trait]
impl RequestHandler for Inner {
    type Identity = HelperIdentity;

    async fn handle(
        &self,
        req: Addr<Self::Identity>,
        data: BodyStream,
    ) -> Result<HelperResponse, ApiError> {
        fn ext_query_id(req: &Addr<HelperIdentity>) -> Result<QueryId, ApiError> {
            req.query_id.ok_or_else(|| {
                ApiError::BadRequest("Query input is missing query_id argument".into())
            })
        }

        let qp = &self.query_processor;

        Ok(match req.route {
            r @ RouteId::Records => {
                return Err(ApiError::BadRequest(
                    format!("{r:?} request must not be handled by query processing flow").into(),
                ))
            }
            RouteId::ReceiveQuery => {
                let req = req.into::<QueryConfig>()?;
                HelperResponse::from(
                    qp.new_query(Transport::clone_ref(&self.transport), req)
                        .await?,
                )
            }
            RouteId::PrepareQuery => {
                let req = req.into::<PrepareQuery>()?;
                HelperResponse::from(qp.prepare(&self.transport, req)?)
            }
            RouteId::QueryInput => {
                let query_id = ext_query_id(&req)?;
                HelperResponse::from(qp.receive_inputs(
                    Transport::clone_ref(&self.transport),
                    QueryInput {
                        query_id,
                        input_stream: data,
                    },
                )?)
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
