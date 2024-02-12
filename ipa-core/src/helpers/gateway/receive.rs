use std::any::type_name;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::Bytes;

use dashmap::{mapref::entry::Entry, DashMap};
use futures::Stream;
use futures_util::stream::Fuse;
use pin_project::pin_project;
use typenum::Unsigned;

use crate::{
    helpers::{buffers::UnorderedReceiver, ChannelId, Error, Message, Transport, TransportImpl},
    protocol::RecordId,
};
use crate::ff::Serializable;
use crate::helpers::{HelperIdentity, Role};
use crate::helpers::gateway::transport::RoleResolvingTransport;
use crate::helpers::transport::{BufDeque, ExtendResult, TransportIdentity};
use crate::sharding::ShardId;

use crate::sync::{Arc, Mutex};

pub type RoleIndexedReceiver = UnorderedReceiver<<RoleResolvingTransport as Transport<Role>>::RecordsStream, Vec<u8>>;
pub type ShardRecvStream = Mutex<Fuse<<TransportImpl<ShardId> as Transport<ShardId>>::RecordsStream>>;


#[pin_project]
pub struct ShardReceivingEnd<M> {
    pub(super) my_id: String,
    pub(super) channel_id: ChannelId<ShardId>,
    #[pin]
    pub(super) recv_stream: ShardRecvStream,
    pub(super) buf: BufDeque,
    pub(super) received: usize,
    pub(super) _phantom: PhantomData<M>,
}

/// Receiving end end of the gateway channel.
/// FIXME: role is not transport identity
pub struct ReceivingEnd<O: TransportIdentity, S, M: Send + Serializable + 'static> {
    channel_id: ChannelId<O>,
    recv: S,
    _phantom: PhantomData<M>,
}

/// Receiving channels, indexed by (role, step).
pub(super) struct GatewayReceivers<O: TransportIdentity, R> {
    pub(super) inner: DashMap<ChannelId<O>, R>
}

impl <O: TransportIdentity, R> Default for GatewayReceivers<O, R> {
    fn default() -> Self {
        Self {
            inner: DashMap::default()
        }
    }
}

impl <O: TransportIdentity, S, M: Send + Serializable + 'static> ReceivingEnd<O, S, M> {
    pub(super) fn new(channel_id: ChannelId<O>, rx: S) -> Self {
        Self {
            channel_id,
            recv: rx,
            _phantom: PhantomData,
        }
    }
}


impl <M: Send + Serializable + 'static> Stream for ShardReceivingEnd<M> {
    type Item = Result<M, crate::helpers::Error<ShardId>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(v) = self.buf.try_read() {
                let rid = (self.received).into();
                self.received += 1;
                return Poll::Ready(Some(v.map_err(|e|
                    crate::helpers::Error::SerializationError {
                        record_id: rid,
                        step: self.channel_id.gate.to_string(),
                        inner: Box::new(e),
                    }
                )))
            }

            let mut s = self.recv_stream.lock().unwrap();
            // FIXME - this pin stuff drives me nuts
            let this = unsafe { Pin::new_unchecked(s.deref_mut()) };
            let Poll::Ready(polled_item) = this.poll_next(cx) else {
                return Poll::Pending;
            };

            drop(s);

            match self.buf.extend(polled_item.map(Ok)) {
                ExtendResult::Finished if !self.buf.is_empty() => return Poll::Ready(Some(Err(crate::helpers::Error::ReceiveError {
                    source: self.channel_id.id,
                    step: self.channel_id.gate.to_string(),
                    inner: format!("Stream ended leaving {} unmatched bytes. {} required {} bytes", self.buf.len(), type_name::<M>(), M::Size::USIZE).into(),
                }))),
                ExtendResult::Error(err) => return return Poll::Ready(Some(Err(crate::helpers::Error::ReceiveError {
                    source: self.channel_id.id,
                    step: self.channel_id.gate.to_string(),
                    inner: err.into()
                }))),
                ExtendResult::Ok => (),
                ExtendResult::Finished => return Poll::Ready(None)
            }
        }
    }
}

impl <M: Message> ReceivingEnd<Role, RoleIndexedReceiver, M> {

    /// Receive message associated with the given record id. This method does not return until
    /// message is actually received and deserialized.
    ///
    /// ## Errors
    /// Returns an error if receiving fails
    ///
    /// ## Panics
    /// This will panic if message size does not fit into 8 bytes and it somehow got serialized
    /// and sent to this helper.
    #[tracing::instrument(level = "trace", "receive", skip_all, fields(i = %record_id, from = ?self.channel_id.id, gate = ?self.channel_id.gate.as_ref()))]
    pub async fn receive(&self, record_id: RecordId) -> Result<M, Error<Role>> {
        self.recv
            .recv::<M, _>(record_id)
            .await
            .map_err(|e| Error::ReceiveError {
                source: self.channel_id.id,
                step: self.channel_id.gate.to_string(),
                inner: Box::new(e),
            })
    }
}

impl <O: TransportIdentity, R: Clone> GatewayReceivers<O, R> {
    pub fn get_or_create<F: FnOnce() -> R>(&self, channel_id: &ChannelId<O>, ctr: F) -> R {
        // TODO: raw entry API if it becomes available to avoid cloning the key
        match self.inner.entry(channel_id.clone()) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let stream = ctr();
                entry.insert(stream.clone());

                stream
            }
        }
    }
}
