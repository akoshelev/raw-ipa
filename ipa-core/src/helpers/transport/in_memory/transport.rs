use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    convert,
    fmt::{Debug, Formatter},
    io,
    pin::Pin,
    task::{Context, Poll},
};
use std::hash::Hash;

use ::tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
#[cfg(all(feature = "shuttle", test))]
use shuttle::future as tokio;
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;

use crate::{
    error::BoxError,
    helpers::{
        query::{PrepareQuery, QueryConfig},
        HelperIdentity, NoResourceIdentifier, QueryIdBinding, ReceiveRecords, RouteId, RouteParams,
        StepBinding, StreamCollection, Transport, TransportCallbacks,
    },
    protocol::{step::Gate, QueryId},
    sync::{Arc, Weak},
};
use crate::helpers::transport::TransportIdentity;
use crate::sharding::ShardId;


pub(super) type Packet<O> = (Addr<O>, InMemoryStream, oneshot::Sender<Result<(), Error<O>>>);
pub(super) type ConnectionTx<O> = Sender<Packet<O>>;
pub(super) type ConnectionRx<O> = Receiver<Packet<O>>;
type StreamItem = Vec<u8>;

#[derive(Debug, thiserror::Error)]
pub enum Error<O> {
    #[error(transparent)]
    Io {
        #[from]
        inner: io::Error,
    },
    #[error("Request rejected by remote {dest:?}: {inner:?}")]
    Rejected {
        dest: O,
        #[source]
        inner: BoxError,
    },
}

/// In-memory implementation of [`Transport`] backed by Tokio mpsc channels.
/// Use [`Setup`] to initialize it and call [`Setup::start`] to make it actively listen for
/// incoming messages.
pub struct InMemoryTransport<O> {
    identity: O,
    connections: HashMap<O, ConnectionTx<O>>,
    record_streams: StreamCollection<O, InMemoryStream>,
}

impl <O: TransportIdentity> InMemoryTransport<O> {
    #[must_use]
    fn new(identity: O, connections: HashMap<O, ConnectionTx<O>>) -> Self {
        Self {
            identity,
            connections,
            record_streams: StreamCollection::default(),
        }
    }

    #[must_use]
    pub fn identity(&self) -> O {
        self.identity
    }

    /// TODO: maybe it shouldn't be active, but rather expose a method that takes the next message
    /// out and processes it, the same way as query processor does. That will allow all tasks to be
    /// created in one place (driver). It does not affect the [`Transport`] interface,
    /// so I'll leave it as is for now.
    fn listen(self: &Arc<Self>, callbacks: TransportCallbacks<Weak<Self>>, mut rx: ConnectionRx<O>) {
        tokio::spawn(
            {
                let streams = self.record_streams.clone();
                let this = Arc::downgrade(self);
                let dest = Self::identity(&self);
                async move {
                    let mut active_queries = HashSet::new();
                    while let Some((addr, stream, ack)) = rx.recv().await {
                        tracing::trace!("received new message: {addr:?}");

                        let result = match addr.route {
                            RouteId::ReceiveQuery => {
                                let qc = addr.into::<QueryConfig>();
                                (callbacks.receive_query)(Clone::clone(&this), qc)
                                    .await
                                    .map(|query_id| {
                                        assert!(
                                            active_queries.insert(query_id),
                                            "the same query id {query_id:?} is generated twice"
                                        );
                                    })
                                    .map_err(|e| Error::Rejected {
                                        dest,
                                        inner: Box::new(e),
                                    })
                            }
                            RouteId::Records => {
                                let query_id = addr.query_id.unwrap();
                                let gate = addr.gate.unwrap();
                                let from = addr.origin.unwrap();
                                streams.add_stream((query_id, from, gate), stream);
                                Ok(())
                            }
                            RouteId::PrepareQuery => {
                                let input = addr.into::<PrepareQuery>();
                                (callbacks.prepare_query)(Clone::clone(&this), input)
                                    .await
                                    .map_err(|e| Error::Rejected {
                                        dest,
                                        inner: Box::new(e),
                                    })
                            }
                        };

                        ack.send(result).unwrap();
                    }
                }
            }
            .instrument(tracing::info_span!("transport_loop", id=?self.identity).or_current()),
        );
    }

    pub fn get_channel(&self, dest: O) -> ConnectionTx<O> {
        if dest == self.identity() {
            panic!("Can't open a channel to myself: {dest:?}");
        }

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
impl <O: TransportIdentity> Transport<O> for Weak<InMemoryTransport<O>> {
    type RecordsStream = ReceiveRecords<O, InMemoryStream>;
    type Error = Error<O>;

    fn identity(&self) -> O {
        self.upgrade().unwrap().identity
    }

    async fn send<
        D: Stream<Item = Vec<u8>> + Send + 'static,
        Q: QueryIdBinding,
        S: StepBinding,
        R: RouteParams<RouteId, Q, S>,
    >(
        &self,
        dest: O,
        route: R,
        data: D,
    ) -> Result<(), Error<O>>
    where
        Option<QueryId>: From<Q>,
        Option<Gate>: From<S>,
    {
        let this = self.upgrade().unwrap();
        let channel = this.get_channel(dest);
        let addr = Addr::from_route(this.identity, route);
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
            })
            .and_then(convert::identity)
    }

    fn receive<R: RouteParams<NoResourceIdentifier, QueryId, Gate>>(
        &self,
        from: O,
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

pub(super) struct Addr<O> {
    route: RouteId,
    origin: Option<O>,
    query_id: Option<QueryId>,
    gate: Option<Gate>,
    params: String,
}

impl <O> Addr<O> {
    #[allow(clippy::needless_pass_by_value)] // to avoid using double-reference at callsites
    pub(super) fn from_route<Q: QueryIdBinding, S: StepBinding, R: RouteParams<RouteId, Q, S>>(
        origin: O,
        route: R,
    ) -> Self
    where
        Option<QueryId>: From<Q>,
        Option<Gate>: From<S>,
    {
        Self {
            route: route.resource_identifier(),
            origin: Some(origin),
            query_id: route.query_id().into(),
            gate: route.gate().into(),
            params: route.extra().borrow().to_string(),
        }
    }

    fn into<T: DeserializeOwned>(self) -> T {
        serde_json::from_str(&self.params).unwrap()
    }

    #[cfg(all(test, unit_test))]
    fn records(from: O, query_id: QueryId, gate: Gate) -> Self {
        Self {
            route: RouteId::Records,
            origin: Some(from),
            query_id: Some(query_id),
            gate: Some(gate),
            params: String::new(),
        }
    }
}

impl <O> Debug for Addr<O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Addr[route={:?}, query_id={:?}, step={:?}, params={}]",
            self.route, self.query_id, self.gate, self.params
        )
    }
}

pub struct Setup<O: TransportIdentity> {
    identity: O,
    tx: ConnectionTx<O>,
    rx: ConnectionRx<O>,
    connections: HashMap<O, ConnectionTx<O>>,
}

impl <O: TransportIdentity> Setup<O> {
    #[must_use]
    pub fn new(identity: O) -> Self {
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

    fn into_active_conn(
        self,
        callbacks: TransportCallbacks<Weak<InMemoryTransport<O>>>,
    ) -> (ConnectionTx<O>, Arc<InMemoryTransport<O>>) {
        let transport = Arc::new(InMemoryTransport::new(self.identity, self.connections));
        transport.listen(callbacks, self.rx);

        (self.tx, transport)
    }

    #[must_use]
    pub fn start(
        self,
        callbacks: TransportCallbacks<Weak<InMemoryTransport<O>>>,
    ) -> Arc<InMemoryTransport<O>> {
        self.into_active_conn(callbacks).1
    }
}

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

    use futures_util::{stream::poll_immediate, FutureExt, StreamExt};
    use tokio::sync::{mpsc::channel, oneshot};

    use crate::{
        ff::{FieldType, Fp31},
        helpers::{
            query::{QueryConfig, QueryType::TestMultiply},
            transport::in_memory::{
                transport::{Addr, ConnectionTx, Error, InMemoryStream, InMemoryTransport},
                InMemoryNetwork, Setup,
            },
            HelperIdentity, OrderingSender, RouteId, Transport, TransportCallbacks,
        },
        protocol::{step::Gate, QueryId},
        sync::Arc,
    };
    use crate::helpers::transport::TransportIdentity;

    const STEP: &str = "in-memory-transport";

    async fn send_and_ack<O: TransportIdentity>(sender: &ConnectionTx<O>, addr: Addr<O>, data: InMemoryStream) {
        let (tx, rx) = oneshot::channel();
        sender.send((addr, data, tx)).await.unwrap();
        rx.await
            .map_err(|_e| Error::Io {
                inner: io::Error::new(ErrorKind::ConnectionRefused, "channel closed"),
            })
            .and_then(convert::identity)
            .unwrap();
    }

    #[tokio::test]
    async fn callback_is_called() {
        let (signal_tx, signal_rx) = oneshot::channel();
        let signal_tx = Arc::new(Mutex::new(Some(signal_tx)));
        let (tx, _transport) =
            Setup::new(HelperIdentity::ONE).into_active_conn(TransportCallbacks {
                receive_query: Box::new(move |_transport, query_config| {
                    let signal_tx = Arc::clone(&signal_tx);
                    Box::pin(async move {
                        // this works because callback is only called once
                        signal_tx
                            .lock()
                            .unwrap()
                            .take()
                            .expect("query callback invoked more than once")
                            .send(query_config)
                            .unwrap();
                        Ok(QueryId)
                    })
                }),
                ..Default::default()
            });
        let expected = QueryConfig::new(TestMultiply, FieldType::Fp32BitPrime, 1u32).unwrap();

        send_and_ack(
            &tx,
            Addr::from_route(HelperIdentity::TWO, &expected),
            InMemoryStream::empty(),
        )
        .await;

        assert_eq!(expected, signal_rx.await.unwrap());
    }

    #[tokio::test]
    async fn receive_not_ready() {
        let (tx, transport) =
            Setup::new(HelperIdentity::ONE).into_active_conn(TransportCallbacks::default());
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
            Setup::new(HelperIdentity::ONE).into_active_conn(TransportCallbacks::default());
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

        let transport1 = setup1.start(TransportCallbacks::default());
        let transport2 = setup2.start(TransportCallbacks::default());
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
            Setup::new(HelperIdentity::ONE).into_active_conn(TransportCallbacks::default());
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
        let network = InMemoryNetwork::default();
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
