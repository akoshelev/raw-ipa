use std::{
    collections::HashMap,
    convert,
    fmt::{Debug, Formatter},
    io,
    pin::Pin,
    task::{Context, Poll},
};

use ::tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
#[cfg(all(feature = "shuttle", test))]
use shuttle::future as tokio;
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;

use crate::{
    error::BoxError,
    helpers::{
        HelperIdentity,
        NoResourceIdentifier, QueryIdBinding, ReceiveRecords, RouteParams, StepBinding,
        StreamCollection, Transport, TransportIdentity,
    },
    protocol::{QueryId, step::Gate},
    sharding::ShardIndex,
    sync::{Arc, Weak},
};
use crate::helpers::{ApiError, BodyStream, HelperResponse, PanickingHandler, RequestHandler};
use crate::helpers::transport::routing::{Addr, RouteId};

type Packet<I> = (
    Addr<I>,
    InMemoryStream,
    oneshot::Sender<Result<HelperResponse, ApiError>>,
);
type ConnectionTx<I> = Sender<Packet<I>>;
type ConnectionRx<I> = Receiver<Packet<I>>;
type StreamItem = Vec<u8>;

#[derive(Debug, thiserror::Error)]
pub enum Error<I> {
    #[error(transparent)]
    Io {
        #[from]
        inner: io::Error,
    },
    #[error("Request rejected by remote {dest:?}: {inner:?}")]
    Rejected {
        dest: I,
        #[source]
        inner: BoxError,
    },
    #[error(transparent)]
    DeserializationFailed {
        #[from]
        inner: serde_json::Error
    }
}

/// In-memory implementation of [`Transport`] backed by Tokio mpsc channels.
/// Use [`Setup`] to initialize it and call [`Setup::start`] to make it actively listen for
/// incoming messages.
pub struct InMemoryTransport<I> {
    identity: I,
    connections: HashMap<I, ConnectionTx<I>>,
    record_streams: StreamCollection<I, InMemoryStream>,
}

impl<I: TransportIdentity> InMemoryTransport<I> {
    #[must_use]
    fn new(identity: I, connections: HashMap<I, ConnectionTx<I>>) -> Self {
        Self {
            identity,
            connections,
            record_streams: StreamCollection::default(),
        }
    }

    #[must_use]
    pub fn identity(&self) -> I {
        self.identity
    }

    /// TODO: maybe it shouldn't be active, but rather expose a method that takes the next message
    /// out and processes it, the same way as query processor does. That will allow all tasks to be
    /// created in one place (driver). It does not affect the [`Transport`] interface,
    /// so I'll leave it as is for now.
    fn listen(
        self: &Arc<Self>,
        handler: impl RequestHandler<Identity = I>,
        mut rx: ConnectionRx<I>,
    ) {
        tokio::spawn(
            {
                let streams = self.record_streams.clone();
                let this = Arc::downgrade(self);
                async move {
                    while let Some((addr, stream, ack)) = rx.recv().await {
                        tracing::trace!("received new message: {addr:?}");

                        let result = match addr.route {
                            RouteId::Records => {
                                let query_id = addr.query_id.unwrap();
                                let gate = addr.gate.unwrap();
                                let from = addr.origin.unwrap();
                                streams.add_stream((query_id, from, gate), stream);
                                Ok(HelperResponse::ok())
                            }
                            RouteId::ReceiveQuery | RouteId::PrepareQuery | RouteId::QueryInput | RouteId::QueryStatus | RouteId::CompleteQuery => {
                                handler.handle(addr, BodyStream::from_infallible(stream.map(Vec::into_boxed_slice))).await
                                // callbacks.handle(Clone::clone(&this), addr).await
                            }
                        };

                        ack.send(result).unwrap();
                    }
                }
            }
            .instrument(tracing::info_span!("transport_loop", id=?self.identity).or_current()),
        );
    }

    fn get_channel(&self, dest: I) -> ConnectionTx<I> {
        self.connections
            .get(&dest)
            .unwrap_or_else(|| {
                panic!(
                    "Should have an active connection from {:?} to {:?}",
                    self.identity, dest
                );
            })
            .clone()
    }

    /// Resets this transport, making it forget its state and be ready for processing another query.
    pub fn reset(&self) {
        self.record_streams.clear();
    }
}

#[async_trait]
impl<I: TransportIdentity> Transport for Weak<InMemoryTransport<I>> {
    type Identity = I;
    type RecordsStream = ReceiveRecords<I, InMemoryStream>;
    type Error = Error<I>;

    fn identity(&self) -> I {
        self.upgrade().unwrap().identity
    }

    async fn send<
        D: Stream<Item = Vec<u8>> + Send + 'static,
        Q: QueryIdBinding,
        S: StepBinding,
        R: RouteParams<RouteId, Q, S>,
    >(
        &self,
        dest: I,
        route: R,
        data: D,
    ) -> Result<(), Error<I>>
    where
        Option<QueryId>: From<Q>,
        Option<Gate>: From<S>,
    {
        let this = self.upgrade().unwrap();
        let channel = this.get_channel(dest);
        let addr = Addr::from_route(Some(this.identity), route);
        let (ack_tx, ack_rx) = oneshot::channel();

        channel
            .send((addr, InMemoryStream::wrap(data), ack_tx))
            .await
            .map_err(|_e| {
                io::Error::new::<String>(io::ErrorKind::ConnectionAborted, "channel closed".into())
            })?;

        ack_rx
            .await
            .map_err(|_recv_error| Error::Rejected {
                dest,
                inner: "channel closed".into(),
            })?.map_err(|e| Error::Rejected { dest, inner: e.into() }).map(|_| ())
    }

    fn receive<R: RouteParams<NoResourceIdentifier, QueryId, Gate>>(
        &self,
        from: I,
        route: R,
    ) -> Self::RecordsStream {
        ReceiveRecords::new(
            (route.query_id(), from, route.gate()),
            self.upgrade().unwrap().record_streams.clone(),
        )
    }
}

/// Convenience struct to support heterogeneous in-memory streams
pub struct InMemoryStream {
    /// There is only one reason for this to have dynamic dispatch: tests that use from_iter method.
    inner: Pin<Box<dyn Stream<Item = StreamItem> + Send>>,
}

impl InMemoryStream {
    #[cfg(all(test, unit_test))]
    fn empty() -> Self {
        Self::from_iter(std::iter::empty())
    }

    fn wrap<S: Stream<Item = StreamItem> + Send + 'static>(value: S) -> Self {
        Self {
            inner: Box::pin(value),
        }
    }

    #[cfg(all(test, unit_test))]
    fn from_iter<I>(input: I) -> Self
    where
        I: IntoIterator<Item = StreamItem>,
        I::IntoIter: Send + 'static,
    {
        use futures_util::stream;
        Self {
            inner: Box::pin(stream::iter(input)),
        }
    }
}

impl From<Receiver<StreamItem>> for InMemoryStream {
    fn from(value: Receiver<StreamItem>) -> Self {
        Self {
            inner: Box::pin(ReceiverStream::new(value)),
        }
    }
}

impl Stream for InMemoryStream {
    type Item = StreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::get_mut(self);
        this.inner.poll_next_unpin(cx)
    }
}

impl Debug for InMemoryStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InMemoryStream")
    }
}

pub struct Setup<I> {
    identity: I,
    tx: ConnectionTx<I>,
    rx: ConnectionRx<I>,
    connections: HashMap<I, ConnectionTx<I>>,
}

impl<I: TransportIdentity> Setup<I> {
    #[must_use]
    pub fn new(identity: I) -> Self {
        let (tx, rx) = channel(16);
        Self {
            identity,
            tx,
            rx,
            connections: HashMap::default(),
        }
    }

    /// Establishes a link between this helper and another one
    ///
    /// ## Panics
    /// Panics if there is a link already.
    pub fn connect(&mut self, other: &mut Self) {
        assert!(self
            .connections
            .insert(other.identity, other.tx.clone())
            .is_none());
        assert!(other
            .connections
            .insert(self.identity, self.tx.clone())
            .is_none());
    }


    pub(crate) fn start(self, handler: impl RequestHandler<Identity = I>) -> Arc<InMemoryTransport<I>> {
        self.into_active_conn(handler).1
    }

    pub fn start_no_handler(self) -> Arc<InMemoryTransport<I>> {
        self.into_active_conn(PanickingHandler::default()).1
    }

    fn into_active_conn(
        self,
        handler: impl RequestHandler<Identity = I>,
    ) -> (ConnectionTx<I>, Arc<InMemoryTransport<I>>)
    {
        let transport = Arc::new(InMemoryTransport::new(self.identity, self.connections));
        transport.listen(handler, self.rx);

        (self.tx, transport)
    }
}

// /// Trait to tie up different transports to the requests handlers they can use inside their
// /// listen loop.
// pub trait ListenerSetup {
//     type Identity: TransportIdentity;
//     type Handler: RequestHandler<Self::Identity> + 'static;
//     type Listener;
//
//     fn start<I: Into<Self::Handler>>(self, handler: I) -> Self::Listener;
// }
//
// impl ListenerSetup for Setup<HelperIdentity> {
//     type Identity = HelperIdentity;
//     type Handler = HelperRequestHandler;
//     type Listener = Arc<InMemoryTransport<Self::Identity>>;
//
//     fn start<I: Into<Self::Handler>>(self, handler: I) -> Self::Listener {
//         self.into_active_conn(handler).1
//     }
// }
//
// impl ListenerSetup for Setup<ShardIndex> {
//     type Identity = ShardIndex;
//     type Handler = ();
//     type Listener = Arc<InMemoryTransport<Self::Identity>>;
//
//     fn start<I: Into<Self::Handler>>(self, handler: I) -> Self::Listener {
//         self.into_active_conn(handler).1
//     }
// }

#[cfg(all(test, unit_test))]
mod tests {
    use std::{
        collections::HashMap,
        convert, io,
        io::ErrorKind,
        num::NonZeroUsize,
        panic::AssertUnwindSafe,
        sync::{Mutex, Weak},
        task::Poll,
    };

    use futures_util::{FutureExt, stream::poll_immediate, StreamExt};
    use tokio::sync::{mpsc::channel, oneshot};

    use crate::{
        ff::{FieldType, Fp31},
        helpers::{
            HelperIdentity,
            OrderingSender,
            query::{QueryConfig, QueryType::TestMultiply}, Transport, transport::in_memory::{
                InMemoryMpcNetwork,
                Setup, transport::{
                    Addr, ConnectionTx, Error, InMemoryStream, InMemoryTransport,
                },
            },
            TransportIdentity,
        },
        protocol::{QueryId, step::Gate},
        sync::Arc,
    };
    use crate::helpers::{HelperResponse, PanickingHandler, Role, RoleAssignment};
    use crate::helpers::query::PrepareQuery;
    use crate::helpers::transport::routing::RouteId;

    const STEP: &str = "in-memory-transport";

    async fn send_and_ack<I: TransportIdentity>(
        sender: &ConnectionTx<I>,
        addr: Addr<I>,
        data: InMemoryStream,
    ) {
        let (tx, rx) = oneshot::channel();
        sender.send((addr, data, tx)).await.unwrap();
        let _ = rx.await
            .map_err(|_e| Error::<I>::Io {
                inner: io::Error::new(ErrorKind::ConnectionRefused, "channel closed"),
            }).unwrap().unwrap();
    }

    #[tokio::test]
    async fn handler_is_called() {
        let (signal_tx, signal_rx) = oneshot::channel();
        let signal_tx = Arc::new(Mutex::new(Some(signal_tx)));
        let (tx, _transport) =
        // TransportCallbacks {
        //         receive_query: Box::new(move |_transport, query_config| {
        //             let signal_tx = Arc::clone(&signal_tx);
        //             Box::pin(async move {
        //             })
        //         }),
        //         ..Default::default()
        //     }
            Setup::new(HelperIdentity::ONE).into_active_conn(move |addr: Addr<HelperIdentity>, stream| {
                    let RouteId::ReceiveQuery = addr.route else {
                        panic!("unexpected call: {addr:?}")
                    };
                    let query_config = addr.into::<QueryConfig>().unwrap();

                    // this works because callback is only called once
                    signal_tx
                        .lock()
                        .unwrap()
                        .take()
                        .expect("query callback invoked more than once")
                        .send(query_config.clone())
                        .unwrap();
                    Ok(HelperResponse::from(PrepareQuery {
                        query_id: QueryId,
                        config: query_config,
                        roles: RoleAssignment::try_from([Role::H1, Role::H2, Role::H3]).unwrap()
                    }))
                });
        let expected = QueryConfig::new(TestMultiply, FieldType::Fp32BitPrime, 1u32).unwrap();

        send_and_ack(
            &tx,
            Addr::from_route(Some(HelperIdentity::TWO), &expected),
            InMemoryStream::empty(),
        )
        .await;

        assert_eq!(expected, signal_rx.await.unwrap());
    }

    #[tokio::test]
    async fn receive_not_ready() {
        let (tx, transport) =
            Setup::new(HelperIdentity::ONE).into_active_conn(PanickingHandler::default());
        let transport = Arc::downgrade(&transport);
        let expected = vec![vec![1], vec![2]];

        let mut stream = transport.receive(HelperIdentity::TWO, (QueryId, Gate::from(STEP)));

        // make sure it is not ready as it hasn't received the records stream yet.
        assert!(matches!(
            poll_immediate(&mut stream).next().await,
            Some(Poll::Pending)
        ));
        send_and_ack(
            &tx,
            Addr::records(HelperIdentity::TWO, QueryId, Gate::from(STEP)),
            InMemoryStream::from_iter(expected.clone()),
        )
        .await;

        assert_eq!(expected, stream.collect::<Vec<_>>().await);
    }

    #[tokio::test]
    async fn receive_ready() {
        let (tx, transport) =
            Setup::new(HelperIdentity::ONE).into_active_conn(PanickingHandler::default());
        let expected = vec![vec![1], vec![2]];

        send_and_ack(
            &tx,
            Addr::records(HelperIdentity::TWO, QueryId, Gate::from(STEP)),
            InMemoryStream::from_iter(expected.clone()),
        )
        .await;

        let stream =
            Arc::downgrade(&transport).receive(HelperIdentity::TWO, (QueryId, Gate::from(STEP)));

        assert_eq!(expected, stream.collect::<Vec<_>>().await);
    }

    #[tokio::test]
    async fn two_helpers() {
        async fn send_and_verify(
            from: HelperIdentity,
            to: HelperIdentity,
            transports: &HashMap<HelperIdentity, Weak<InMemoryTransport<HelperIdentity>>>,
        ) {
            let (stream_tx, stream_rx) = channel(1);
            let stream = InMemoryStream::from(stream_rx);

            let from_transport = transports.get(&from).unwrap();
            let to_transport = transports.get(&to).unwrap();
            let gate = Gate::from(STEP);

            let mut recv = to_transport.receive(from, (QueryId, gate.clone()));
            assert!(matches!(
                poll_immediate(&mut recv).next().await,
                Some(Poll::Pending)
            ));

            from_transport
                .send(to, (RouteId::Records, QueryId, gate.clone()), stream)
                .await
                .unwrap();
            stream_tx.send(vec![1, 2, 3]).await.unwrap();
            assert_eq!(vec![1, 2, 3], recv.next().await.unwrap());
            assert!(matches!(
                poll_immediate(&mut recv).next().await,
                Some(Poll::Pending)
            ));

            stream_tx.send(vec![4, 5, 6]).await.unwrap();
            assert_eq!(vec![4, 5, 6], recv.next().await.unwrap());
            assert!(matches!(
                poll_immediate(&mut recv).next().await,
                Some(Poll::Pending)
            ));

            drop(stream_tx);
            assert!(poll_immediate(&mut recv).next().await.is_none());
        }

        let mut setup1 = Setup::new(HelperIdentity::ONE);
        let mut setup2 = Setup::new(HelperIdentity::TWO);

        setup1.connect(&mut setup2);

        let transport1 = setup1.start(PanickingHandler::default());
        let transport2 = setup2.start(PanickingHandler::default());
        let transports = HashMap::from([
            (HelperIdentity::ONE, Arc::downgrade(&transport1)),
            (HelperIdentity::TWO, Arc::downgrade(&transport2)),
        ]);

        send_and_verify(HelperIdentity::ONE, HelperIdentity::TWO, &transports).await;
        send_and_verify(HelperIdentity::TWO, HelperIdentity::ONE, &transports).await;
    }

    #[tokio::test]
    async fn panic_if_stream_received_twice() {
        let (tx, owned_transport) =
            Setup::new(HelperIdentity::ONE).into_active_conn(PanickingHandler::default());
        let gate = Gate::from(STEP);
        let (stream_tx, stream_rx) = channel(1);
        let stream = InMemoryStream::from(stream_rx);
        let transport = Arc::downgrade(&owned_transport);

        let mut recv_stream = transport.receive(HelperIdentity::TWO, (QueryId, gate.clone()));
        send_and_ack(
            &tx,
            Addr::records(HelperIdentity::TWO, QueryId, gate.clone()),
            stream,
        )
        .await;

        stream_tx.send(vec![4, 5, 6]).await.unwrap();
        assert_eq!(vec![4, 5, 6], recv_stream.next().await.unwrap());

        // the same stream cannot be received again
        let mut err_recv = transport.receive(HelperIdentity::TWO, (QueryId, gate.clone()));
        let err = AssertUnwindSafe(err_recv.next()).catch_unwind().await;
        assert_eq!(
            Some(true),
            err.unwrap_err()
                .downcast_ref::<String>()
                .map(|s| { s.contains("stream has been consumed already") })
        );

        // even after the input stream is closed
        drop(stream_tx);
        let mut err_recv = transport.receive(HelperIdentity::TWO, (QueryId, gate.clone()));
        let err = AssertUnwindSafe(err_recv.next()).catch_unwind().await;
        assert_eq!(
            Some(true),
            err.unwrap_err()
                .downcast_ref::<String>()
                .map(|s| { s.contains("stream has been consumed already") })
        );
    }

    #[tokio::test]
    async fn can_consume_ordering_sender() {
        let tx = Arc::new(OrderingSender::new(NonZeroUsize::new(2).unwrap(), 2));
        let rx = Arc::clone(&tx).as_rc_stream();
        let network = InMemoryMpcNetwork::default();
        let transport1 = network.transport(HelperIdentity::ONE);
        let transport2 = network.transport(HelperIdentity::TWO);

        let gate = Gate::from(STEP);
        transport1
            .send(
                HelperIdentity::TWO,
                (RouteId::Records, QueryId, gate.clone()),
                rx,
            )
            .await
            .unwrap();
        let mut recv = transport2.receive(HelperIdentity::ONE, (QueryId, gate));

        tx.send(0, Fp31::try_from(0_u128).unwrap()).await;
        // can't receive the value at index 0 because of buffering inside the sender
        assert_eq!(Some(Poll::Pending), poll_immediate(&mut recv).next().await);

        // make the sender ready
        tx.send(1, Fp31::try_from(1_u128).unwrap()).await;
        tx.close(2).await;
        // drop(tx);

        // must be received by now
        assert_eq!(vec![vec![0, 1]], recv.collect::<Vec<_>>().await);
    }
}
