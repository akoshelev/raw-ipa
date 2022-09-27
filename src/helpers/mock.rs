///! Provides an implementation of `Gateway` and `Mesh` suitable for unit tests.
use crate::helpers::error::Error;
use crate::helpers::mesh::{Gateway, Mesh, Message};
use crate::helpers::Identity;
use crate::protocol::{RecordId, Step};
use async_trait::async_trait;
use futures::Stream;
use futures_util::stream::SelectAll;
use futures_util::StreamExt;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};
use smallvec::SmallVec;

use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;
use crate::field::{Field, Int};

/// Gateway is just the proxy for `Controller` interface to provide stable API and hide
/// `Controller`'s dependencies
#[derive(Debug)]
pub struct TestHelperGateway<S> {
    controller: Controller<S>,
}

/// This is the communication end exposed to protocols to send messages between helpers.
/// It locks in the step, so all information sent through it is implicitly associated with
/// the step used to create this instance. Along with `QueryId` that is used to create the
/// test world, it is used to uniquely identify the "stream" of records flowing between
/// helper instances
#[derive(Debug)]
pub struct TestMesh<S> {
    step: S,
    controller: Controller<S>,
}

/// Represents control messages sent between helpers to handle infrastructure requests.
enum ControlMessage<S> {
    /// Connection for step S is requested by the peer
    ConnectionRequest(Identity, S, mpsc::Receiver<MessageEnvelope>),
}

type MessagePayload = SmallVec<[u8; 1]>;

#[derive(Debug)]
struct MessageEnvelope {
    record_id: RecordId,
    payload: MessagePayload,
}

/// Combination of helper identity and step that uniquely identifies a single channel of communication
/// between two helpers.
type ChannelId<S> = (Identity, S);

/// Local buffer for messages that are either awaiting requests to receive them or requests
/// that are pending message reception.
/// Right now it is backed by a hashmap but `SipHash` (default hasher) performance is not great
/// when protection against collisions is not required, so either use a vector indexed by
/// an offset + record or [xxHash](https://github.com/Cyan4973/xxHash)
#[derive(Debug, Default)]
struct MessageBuffer {
    buf: HashMap<RecordId, BufItem>,
}

#[derive(Debug)]
enum BufItem {
    /// There is an outstanding request to receive the message but this helper hasn't seen it yet
    Requested(oneshot::Sender<MessagePayload>),
    /// Message has been received but nobody requested it yet
    Received(MessagePayload),
}

struct ReceiveRequest<S> {
    from: Identity,
    step: S,
    record_id: RecordId,
    sender: oneshot::Sender<MessagePayload>,
}

/// Controller that is created per test helper. Handles control messages and establishes
/// connections between this helper and others. Also keeps the queues of incoming messages
/// indexed by source + step.
#[derive(Debug)]
struct Controller<S> {
    identity: Identity,
    peers: HashMap<Identity, mpsc::Sender<ControlMessage<S>>>,
    connections: Arc<Mutex<HashMap<ChannelId<S>, mpsc::Sender<MessageEnvelope>>>>,
    receive_request_sender: mpsc::Sender<ReceiveRequest<S>>,
}

impl MessageBuffer {
    /// Process request to receive a message with the given `RecordId`.
    fn receive_request(&mut self, record_id: RecordId, s: oneshot::Sender<MessagePayload>) {
        match self.buf.entry(record_id) {
            Entry::Occupied(entry) => match entry.remove() {
                BufItem::Requested(_) => {
                    panic!("More than one request to receive a message for {record_id:?}");
                }
                BufItem::Received(payload) => {
                    s.send(payload).unwrap_or_else(|_| {
                        tracing::warn!("No listener for message {record_id:?}");
                    });
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(BufItem::Requested(s));
            }
        }
    }

    /// Process message that has been received
    fn receive_message(&mut self, msg: MessageEnvelope) {
        match self.buf.entry(msg.record_id) {
            Entry::Occupied(entry) => match entry.remove() {
                BufItem::Requested(s) => {
                    s.send(msg.payload).unwrap_or_else(|_| {
                        tracing::warn!("No listener for message {:?}", msg.record_id);
                    });
                }
                BufItem::Received(_) => {
                    panic!("Duplicate message for the same record {:?}", msg.record_id);
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(BufItem::Received(msg.payload));
            }
        }
    }
}

impl<S: Step> ReceiveRequest<S> {
    pub fn new(
        from: Identity,
        step: S,
        record_id: RecordId,
        sender: oneshot::Sender<MessagePayload>,
    ) -> Self {
        Self {
            from,
            step,
            record_id,
            sender,
        }
    }

    pub fn channel_id(&self) -> ChannelId<S> {
        (self.from, self.step)
    }
}

impl<S: Step> TestHelperGateway<S> {
    fn new(controller: Controller<S>) -> Self {
        Self { controller }
    }

    pub fn make_three() -> [TestHelperGateway<S>; 3] {
        make_controllers().map(Self::new)
    }
}

impl<S: Step> Gateway<TestMesh<S>, S> for TestHelperGateway<S> {
    fn get_channel(&self, step: S) -> TestMesh<S> {
        TestMesh {
            step,
            controller: self.controller.clone(),
        }
    }
}

#[async_trait]
impl<S: Step> Mesh for TestMesh<S> {
    #[tracing::instrument(skip(self), level = "trace")]
    async fn send<T: Message>(
        &mut self,
        target: Identity,
        record_id: RecordId,
        msg: T,
    ) -> Result<(), Error> {
        let sender = self.controller.get_connection(target, self.step).await;

        let mut bytes = SmallVec::new();
        msg.to_smallvec(&mut bytes);
        assert!(!bytes.spilled());
        // let bytes = serde_json::to_vec(&msg).unwrap().into_boxed_slice();
        let envelope = MessageEnvelope {
            record_id,
            payload: bytes,
        };

        sender.send(envelope).await.map_err(|e| Error::SendError {
            dest: target,
            inner: format!("Failed to send {:?}", e.0).into(),
        })?;

        Ok(())
    }

    #[tracing::instrument(skip(self), fields(identity=?self.controller.identity, step=?self.step), level="trace")]
    async fn receive<T: Message>(&mut self, from: Identity, record: RecordId) -> Result<T, Error> {
        let mut payload = self.controller.receive(from, self.step, record).await;
        let obj = T::from_smallvec(&mut payload);
        // let obj: T = serde_json::from_slice(&payload).unwrap();
        tracing::trace!("message received: {obj:?}");

        Ok(obj)
    }

    fn identity(&self) -> Identity {
        self.controller.identity
    }
}

impl<S> Clone for Controller<S> {
    fn clone(&self) -> Self {
        Self {
            receive_request_sender: self.receive_request_sender.clone(),
            identity: self.identity,
            peers: self.peers.clone(),
            connections: Arc::clone(&self.connections),
        }
    }
}

impl<S: Step> Controller<S> {
    fn launch(
        identity: Identity,
        control_tx: HashMap<Identity, mpsc::Sender<ControlMessage<S>>>,
        control_rx: mpsc::Receiver<ControlMessage<S>>,
    ) -> Self {
        let (receive_tx, receive_rx) = mpsc::channel(1);
        let controller = Self {
            receive_request_sender: receive_tx,
            identity,
            connections: Arc::new(Mutex::new(HashMap::new())),
            peers: control_tx,
        };

        Controller::start(identity, control_rx, receive_rx);

        controller
    }

    fn start(
        identity: Identity,
        mut control_rx: mpsc::Receiver<ControlMessage<S>>,
        mut receive_rx: mpsc::Receiver<ReceiveRequest<S>>,
    ) {
        tokio::spawn(async move {
            let mut buf = HashMap::<ChannelId<S>, MessageBuffer>::new();
            let mut channels = SelectAll::new();

            loop {
                // Make a random choice what to process next:
                // * Receive and process a control message
                // * Receive a message from another helper
                // * Handle the request to receive a message from another helper
                tokio::select! {
                    Some(control_message) = control_rx.recv() => {
                        tracing::debug!("new {control_message:?}");
                        match control_message {
                            ControlMessage::ConnectionRequest(peer, step, peer_connection) => {
                                channels.push(prepend((peer, step), ReceiverStream::new(peer_connection)));
                            }
                        }
                    }
                    Some(receive_request) = receive_rx.recv() => {
                        tracing::trace!("new {:?}", receive_request);
                        buf.entry(receive_request.channel_id())
                           .or_default()
                           .receive_request(receive_request.record_id, receive_request.sender);
                    }
                    Some(((from_peer, step), message_envelope)) = channels.next() => {
                        tracing::trace!("new MessageArrival(from={from_peer:?}, step={step:?}, record={:?}, size={}B)", message_envelope.record_id, message_envelope.payload.len());
                        buf.entry((from_peer, step))
                           .or_default()
                           .receive_message(message_envelope);
                    }
                    else => {
                        tracing::debug!("All channels are closed and event loop is terminated");
                        break;
                    }
                }
            }
        }.instrument(tracing::info_span!("helper_event_loop", identity=?identity)));
    }

    #[tracing::instrument(skip(self), level = "trace")]
    async fn get_connection(&self, peer: Identity, step: S) -> mpsc::Sender<MessageEnvelope> {
        assert_ne!(self.identity, peer);

        let (sender, rx) = {
            let mut connections = self.connections.lock().unwrap();
            match connections.entry((peer, step)) {
                Entry::Occupied(entry) => (entry.get().clone(), None),
                Entry::Vacant(entry) => {
                    let (tx, rx) = mpsc::channel(1);
                    (entry.insert(tx).clone(), Some(rx))
                }
            }
        };

        if let Some(rx) = rx {
            tracing::trace!("Requesting connection");
            self.peers
                .get(&peer)
                .expect("peer with id {peer:?} should exist")
                .send(ControlMessage::ConnectionRequest(self.identity, step, rx))
                .await
                .unwrap();
        }

        sender
    }

    async fn receive(&self, peer: Identity, step: S, record: RecordId) -> MessagePayload {
        let (tx, rx) = oneshot::channel();
        self.receive_request_sender
            .send(ReceiveRequest::new(peer, step, record, tx))
            .await
            .unwrap();

        rx.await.unwrap()
    }
}

#[must_use]
fn make_controllers<S: Step>() -> [Controller<S>; 3] {
    let (mut senders, mut receivers) = (HashMap::new(), HashMap::new());
    for identity in Identity::all_variants() {
        let (tx, rx) = mpsc::channel(1);
        senders.insert(*identity, tx);
        receivers.insert(*identity, rx);
    }

    // Every controller gets its own receiver end for control messages
    // and for N party setting gets N-1 senders to communicate these messages to peers
    Identity::all_variants().map(|identity| {
        let peer_senders = senders
            .iter()
            .filter(|(&k, _)| k != identity)
            .map(|(&k, v)| (k, v.clone()))
            .collect::<HashMap<_, _>>();
        let rx = receivers.remove(&identity).unwrap();

        Controller::launch(identity, peer_senders, rx)
    })
}

pub fn prepend<T: Copy + Clone, S: Stream>(id: T, stream: S) -> impl Stream<Item = (T, S::Item)> {
    stream.map(move |item| (id, item))
}

impl<S: Step> Debug for ControlMessage<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlMessage::ConnectionRequest(from, step, _) => {
                write!(f, "ConnectionRequest(from={:?}, step={:?})", from, step)
            }
        }
    }
}

impl<S: Step> Debug for ReceiveRequest<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReceiveRequest(from={:?}, step={:?}, record={:?})",
            self.from, self.step, self.record_id
        )
    }
}
