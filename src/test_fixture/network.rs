use crate::{
    helpers::{
        self,
        network::{ChannelId, MessageChunks, Network, NetworkSink},
        Error, Role,
    },
    protocol::Step,
};
use async_trait::async_trait;
use futures::StreamExt;
use futures_util::stream::{FuturesUnordered, SelectAll};
use std::collections::{hash_map::Entry, HashMap};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;

/// Represents control messages sent between helpers to handle infrastructure requests.
pub(super) enum ControlMessage {
    /// Connection for a step is requested by the peer.
    ConnectionRequest(ChannelId, Receiver<(Vec<u8>, Vec<u32>)>),
}

/// Container for all active helper endpoints
#[derive(Debug)]
pub struct InMemoryNetwork {
    pub endpoints: [Arc<InMemoryEndpoint>; 3],
}

/// Helper endpoint in memory. Capable of opening connections to other helpers and buffering
/// messages it receives from them until someone requests them.
#[derive(Debug)]
pub struct InMemoryEndpoint {
    pub role: Role,
    /// Channels that this endpoint is listening to. There are two helper peers for 3 party setting.
    /// For each peer there are multiple channels open, one per query + step.
    channels: Arc<Mutex<Vec<HashMap<Step, InMemoryChannel>>>>,
    tx: Sender<ControlMessage>,
    rx: Arc<Mutex<Option<Receiver<MessageChunks>>>>,
    network: Weak<InMemoryNetwork>,
    chunks_sender: Sender<MessageChunks>,
}

/// In memory channel is just a standard mpsc channel.
#[derive(Debug, Clone)]
pub struct InMemoryChannel {
    dest: Role,
    tx: Sender<(Vec<u8>, Vec<u32>)>,
}

impl InMemoryNetwork {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new_cyclic(|weak_ptr| {
            let endpoints = Role::all().map(|i| InMemoryEndpoint::new(i, Weak::clone(weak_ptr)));

            Self { endpoints }
        })
    }
}

impl InMemoryEndpoint {
    /// Creates new instance for a given helper role.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(id: Role, world: Weak<InMemoryNetwork>) -> Arc<Self> {
        let (tx, mut open_channel_rx) = mpsc::channel(1);
        let (message_stream_tx, message_stream_rx) = mpsc::channel(1);
        let (chunks_sender, mut chunks_receiver) = mpsc::channel(1);

        let this = Arc::new(Self {
            role: id,
            channels: Arc::new(Mutex::new(vec![
                HashMap::default(),
                HashMap::default(),
                HashMap::default(),
            ])),
            tx,
            rx: Arc::new(Mutex::new(Some(message_stream_rx))),
            network: world,
            chunks_sender,
        });

        tokio::spawn({
            let this = Arc::clone(&this);
            async move {
                let mut peer_channels = SelectAll::new();
                let mut pending_sends = FuturesUnordered::new();
                let mut buf = HashMap::<ChannelId, (Vec<u32>, Vec<u8>)>::new();

                loop {
                    tokio::select! {
                        // handle request to establish connection with a peer
                        Some(control_message) = open_channel_rx.recv() => {
                            match control_message {
                                ControlMessage::ConnectionRequest(channel_id, new_channel_rx) => {
                                    peer_channels.push(ReceiverStream::new(new_channel_rx).map(move |msg| (channel_id.clone(), msg.0, msg.1)));
                                }
                            }
                        }
                        // receive a batch of messages from the peer
                        Some((channel_id, msgs, chunk_ids)) = peer_channels.next() => {
                            tracing::trace!("received chunk {chunk_ids:?} from {channel_id:?}");
                            let entry = buf.entry(channel_id).or_default();

                            entry.0.extend(chunk_ids);
                            entry.1.extend(msgs);
                        }
                        // Handle request to send messages to a peer
                        Some(chunk) = chunks_receiver.recv() => {
                            tracing::trace!("sending chunks {:?} to {:?}", chunk.2, chunk.0);
                            pending_sends.push(this.send_chunk(chunk));
                        }
                        // Drive pending sends to completion
                        Some(_) = pending_sends.next() => {

                        }
                        // If there is nothing else to do, try to obtain a permit to move messages
                        // from the buffer to messaging layer. Potentially we might be thrashing
                        // on permits here.
                        Ok(permit) = message_stream_tx.reserve(), if !buf.is_empty() => {
                            let key = buf.keys().next().unwrap().clone();
                            let (chunks, msgs) = buf.remove(&key).unwrap();

                            tracing::trace!("Gateway receive {chunks:?} from {key:?} please");
                            permit.send((key, msgs, chunks));
                        }
                        else => {
                            break;
                        }
                    }
                }
            }
        }.instrument(tracing::info_span!("in_memory_helper_event_loop", role=?id)));

        this
    }
}

impl InMemoryEndpoint {
    async fn send_chunk(&self, chunk: MessageChunks) -> ChannelId {
        let c = chunk.0.clone();
        let conn = self.get_connection(chunk.0).await;
        conn.send(chunk.1, chunk.2).await.unwrap();
        // conn.send(chunk.2, chunk.1).await.unwrap();

        c
    }

    async fn get_connection(&self, addr: ChannelId) -> InMemoryChannel {
        let mut new_rx = None;

        let channel = {
            let mut channels = self.channels.lock().unwrap();
            let peer_channel = &mut channels[addr.role];

            match peer_channel.entry(addr.step.clone()) {
                Entry::Occupied(entry) => entry.get().clone(),
                Entry::Vacant(entry) => {
                    let (tx, rx) = mpsc::channel(1);
                    let tx = InMemoryChannel {
                        dest: addr.role,
                        tx,
                    };
                    entry.insert(tx.clone());
                    new_rx = Some(rx);

                    tx
                }
            }
        };

        if let Some(rx) = new_rx {
            self.network.upgrade().unwrap().endpoints[addr.role]
                .tx
                .send(ControlMessage::ConnectionRequest(
                    ChannelId::new(self.role, addr.step),
                    rx,
                ))
                .await
                .unwrap();
        }

        channel
    }
}

#[async_trait]
impl Network for Arc<InMemoryEndpoint> {
    type Sink = NetworkSink<MessageChunks>;
    type MessageStream = ReceiverStream<MessageChunks>;

    fn sink(&self) -> Self::Sink {
        let x = self.chunks_sender.clone();
        Self::Sink::new(x)
    }

    fn recv_stream(&self) -> Self::MessageStream {
        let mut rx = self.rx.lock().unwrap();
        if let Some(rx) = rx.take() {
            ReceiverStream::new(rx)
        } else {
            panic!("Message stream has been consumed already");
        }
    }
}

impl InMemoryChannel {
    async fn send(&self, msg: Vec<u8>, chunks: Vec<u32>) -> helpers::Result<()> {
        self.tx
            .send((msg, chunks))
            .await
            .map_err(|e| Error::send_error(self.dest, e))
    }
}

impl Debug for ControlMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlMessage::ConnectionRequest(channel, step) => {
                write!(f, "ConnectionRequest(from={:?}, step={:?})", channel, step)
            }
        }
    }
}
