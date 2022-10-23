//!
//! This module contains implementations and traits that enable protocols to communicate with
//! each other. In order for helpers to send messages, they need to know the destination. In some
//! cases this might be the exact address of helper host/instance (for example IP address), but
//! in many situations MPC helpers simply need to be able to send messages to the
//! corresponding helper without needing to know the exact location - this is what this module
//! enables MPC protocols to do.
//!
use crate::{
    helpers::buffers::{ReceiveBuffer, SendBuffer},
    helpers::error::Error,
    helpers::fabric::{ChannelId, MessageEnvelope, Network},
    helpers::Identity,
    protocol::{RecordId, UniqueStepId},
};

use futures::SinkExt;
use futures::StreamExt;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::{Debug, Formatter};
use std::task::Waker;
use std::time::Duration;
use tokio::sync::{mpsc, Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::Instrument;

/// Trait for messages sent between helpers
pub trait Message: Debug + Send + Serialize + DeserializeOwned + 'static {}

impl<T> Message for T where T: Debug + Send + Serialize + DeserializeOwned + 'static {}

/// Entry point to the messaging layer managing communication channels for protocols and provides
/// the ability to send and receive messages from helper peers. Protocols request communication
/// channels to be open by calling `get_channel`, after that it is possible to send messages
/// through the channel end and request a given message type from helper peer.
///
/// Gateways are generic over `Network` meaning they can operate on top of in-memory communication
/// channels and real network.
///
/// ### Implementation details
/// Gateway, when created, runs an event loop in a dedicated tokio task that pulls the messages
/// from the networking layer and attempts to fulfil the outstanding requests to receive them.
/// If `receive` method on the channel has never been called, it puts the message to the local
/// buffer and keeps it there until such request is made by the protocol.
/// TODO: limit the size of the buffer and only pull messages when there is enough capacity
#[derive(Debug)]
pub struct Gateway<N> {
    role: Identity,
    /// TODO: no need to keep it here if we're happy with its interface
    _network: N,
    /// Sender end of the channel to send requests to receive messages from peers.
    receive_request_tx: mpsc::Sender<ReceiveRequest>,
    envelope_tx: mpsc::Sender<(ChannelId, MessageEnvelope, oneshot::Sender<SendRequestStatus>)>,
    control_handle: JoinHandle<()>,
}

pub(super) enum SendRequestStatus {
    Accepted,
    Rejected(SendRejectionReason)
}

pub(super) enum SendRejectionReason {
    // TODO provide accepted range
    TooFarAhead
}


/// Channel end
#[derive(Debug)]
pub struct Mesh<'a, 'b, N> {
    gateway: &'a Gateway<N>,
    step: &'b UniqueStepId,
    role: Identity,
}

pub(super) struct ReceiveRequest {
    pub channel_id: ChannelId,
    pub record_id: RecordId,
    pub sender: oneshot::Sender<Box<[u8]>>,
}

impl<N: Network> Mesh<'_, '_, N> {
    /// Send a given message to the destination. This method will not return until the message
    /// is delivered to the `Network`.
    ///
    /// # Errors
    /// Returns an error if it fails to send the message or if there is a serialization error.
    pub async fn send<T: Message>(
        &self,
        dest: Identity,
        record_id: RecordId,
        msg: T,
    ) -> Result<(), Error> {
        let bytes = serde_json::to_vec(&msg)
            .map_err(|e| Error::serialization_error(record_id, self.step, e))?
            .into_boxed_slice();

        let envelope = MessageEnvelope {
            record_id,
            payload: bytes,
        };

        self.gateway
            .send(ChannelId::new(dest, self.step.clone()), envelope)
            .await
    }

    /// Receive a message that is associated with the given record id.
    ///
    /// # Errors
    /// Returns an error if it fails to receive the message or if a deserialization error occurred
    pub async fn receive<T: Message>(
        &self,
        source: Identity,
        record_id: RecordId,
    ) -> Result<T, Error> {
        let payload = self
            .gateway
            .receive(ChannelId::new(source, self.step.clone()), record_id)
            .await?;

        let obj: T = serde_json::from_slice(&payload)
            .map_err(|e| Error::serialization_error(record_id, self.step, e))?;

        Ok(obj)
    }

    /// Returns the unique identity of this helper.
    #[must_use]
    pub fn identity(&self) -> Identity {
        self.role
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayConfig {
    /// Maximum number of items to keep inside the buffer before flushing it to network.
    /// Note that this buffer is per channel, so setting it to 10 does not imply that every
    /// 10 messages sent trigger a network request.
    pub send_buffer_capacity: u32,
}

impl<N: Network> Gateway<N> {
    pub fn new(role: Identity, network: N, config: GatewayConfig) -> Self {
        let (tx, mut receive_rx) = mpsc::channel::<ReceiveRequest>(1);
        let (envelope_tx, mut envelope_rx) = mpsc::channel::<(ChannelId, MessageEnvelope, oneshot::Sender<SendRequestStatus>)>(1);
        let mut message_stream = network.recv_stream();
        let mut network_sink = network.sink();

        let control_handle = tokio::spawn(async move {
            // to make forward progress, we periodically check if the system is stalled
            // if nothing happens for long period of time, we try to unblock it by flushing
            // the data that remains inside buffers. Note that the interval picked here is somewhat
            // random - waiting for too long will result in elevated latencies. On the other hand,
            // sending buffers that are half-full will lead to underutilizing the network
            const INTERVAL: Duration = Duration::from_millis(200);

            let mut receive_buf = ReceiveBuffer::default();
            let mut send_buf = SendBuffer::new(config.send_buffer_capacity);

            let sleep = tokio::time::sleep(INTERVAL);
            tokio::pin!(sleep);

            loop {
                // Make a random choice what to process next:
                // * Receive a message from another helper
                // * Handle the request to receive a message from another helper
                // * If send buffer is full, send it down
                tokio::select! {
                    Some(receive_request) = receive_rx.recv() => {
                        tracing::trace!("new {:?}", receive_request);
                        receive_buf.receive_request(receive_request.channel_id, receive_request.record_id, receive_request.sender);
                    }
                    Some((channel_id, messages)) = message_stream.next() => {
                        tracing::trace!("received {} message(s) from {:?}", messages.len(), channel_id);
                        receive_buf.receive_messages(&channel_id, messages);
                    }
                    Some((channel_id, msg, _)) = envelope_rx.recv() => {
                        // todo check for out of range error
                        if let Some(buf_to_send) = send_buf.push(channel_id.clone(), msg).ok().flatten() {
                            tracing::trace!("sending {} message(s) to {:?}", buf_to_send.len(), &channel_id);
                            network_sink.send((channel_id, buf_to_send)).await
                                .expect("Failed to send data to the network");
                        }
                    }
                    _ = &mut sleep, if send_buf.len() > 0 => {
                        let (channel_id, buf_to_send) = send_buf.remove_random();
                        tracing::trace!("sending {} message(s) to {:?}", buf_to_send.len(), channel_id);
                        network_sink.send((channel_id, buf_to_send)).await
                            .expect("Failed to send data to the network");
                    }
                    else => {
                        tracing::debug!("All channels are closed and event loop is terminated");
                        break;
                    }
                }

                // reset the timer as we processed something
                sleep.as_mut().reset(Instant::now() + INTERVAL);
            }
        }.instrument(tracing::info_span!("gateway_loop", identity=?role)));

        Self {
            role,
            _network: network,
            receive_request_tx: tx,
            envelope_tx,
            control_handle,
        }
    }

    /// Create or return an existing channel for a given step. Protocols can send messages to
    /// any helper through this channel (see `Mesh` interface for details).
    ///
    /// This method makes no guarantee that the communication channel will actually be established
    /// between this helper and every other one. The actual connection may be created only when
    /// `Mesh::send` or `Mesh::receive` methods are called.
    pub fn mesh<'a, 'b>(&'a self, step: &'b UniqueStepId) -> Mesh<'a, 'b, N> {
        Mesh {
            gateway: self,
            role: self.role,
            step,
        }
    }

    async fn receive(
        &self,
        channel_id: ChannelId,
        record_id: RecordId,
    ) -> Result<Box<[u8]>, Error> {
        let (tx, rx) = oneshot::channel();
        self.receive_request_tx
            .send(ReceiveRequest {
                channel_id: channel_id.clone(),
                record_id,
                sender: tx,
            })
            .await?;

        rx.await
            .map_err(|e| Error::receive_error(channel_id.identity, e))
    }

    async fn send(&self, id: ChannelId, env: MessageEnvelope) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.envelope_tx.send((id.clone(), env, tx)).await?;

        let status = rx.await
            .map_err(|e| Error::receive_error(id.identity, e))?;
        Ok(())
    }
}

impl<N> Drop for Gateway<N> {
    fn drop(&mut self) {
        self.control_handle.abort();
    }
}

impl Debug for ReceiveRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReceiveRequest({:?}, {:?})",
            self.channel_id, self.record_id
        )
    }
}
