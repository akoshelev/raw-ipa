use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

use crate::{
    error::BoxError,
    helpers::{ChannelId, HelperIdentity, Message, Role, TotalRecords},
    protocol::{step::Gate, RecordId},
};
use crate::helpers::RoleAssignment;
use crate::helpers::transport::TransportIdentity;

/// An error raised by the IPA supporting infrastructure.
#[derive(Error, Debug)]
pub enum Error<O: TransportIdentity> {
    #[error("An error occurred while sending data to {channel:?}: {inner}")]
    SendError {
        channel: ChannelId<O>,

        #[source]
        inner: BoxError,
    },
    #[error("An error occurred while sending data over a reordering channel: {inner}")]
    OrderedChannelError {
        #[source]
        inner: BoxError,
    },
    #[error("An error occurred while sending data to unknown helper: {inner}")]
    PollSendError {
        #[source]
        inner: BoxError,
    },
    #[error("An error occurred while receiving data from {source:?}/{step}: {inner}")]
    ReceiveError {
        source: O,
        step: String,
        #[source]
        inner: BoxError,
    },
    #[error("Expected to receive {record_id:?} but hit end of stream")]
    EndOfStream {
        // TODO(mt): add more fields, like step and role.
        record_id: RecordId,
    },
    #[error("An error occurred while serializing or deserializing data for {record_id:?} and step {step}: {inner}")]
    SerializationError {
        record_id: RecordId,
        step: String,
        #[source]
        inner: BoxError,
    },
    #[error("Encountered unknown identity {0:?}")]
    UnknownIdentity(O),
    #[error("record ID {record_id:?} is out of range for {channel_id:?} (expected {total_records:?} records)")]
    TooManyRecords {
        record_id: RecordId,
        channel_id: ChannelId<O>,
        total_records: TotalRecords,
    },
}

impl Error<Role> {
    pub fn send_error<E: Into<Box<dyn std::error::Error + Send + Sync + 'static>>>(
        channel: ChannelId<Role>,
        inner: E,
    ) -> Self {
        Self::SendError {
            channel,
            inner: inner.into(),
        }
    }

    #[must_use]
    pub fn serialization_error<E: Into<BoxError>>(
        record_id: RecordId,
        gate: &Gate,
        inner: E,
    ) -> Self {
        Self::SerializationError {
            record_id,
            step: String::from(gate.as_ref()),
            inner: inner.into(),
        }
    }

    pub(crate) fn from_identity_error(other: Error<HelperIdentity>, roles: &RoleAssignment) -> Self {
        match other {
            Error::SendError { channel, inner } =>
                Self::SendError { channel: ChannelId::new(roles.role(channel.id), channel.gate), inner },
            Error::OrderedChannelError { inner } => Self::OrderedChannelError { inner },
            Error::PollSendError { inner } => Self::PollSendError { inner },
            Error::ReceiveError { source, step, inner } =>
                Self::ReceiveError { source: roles.role(source), step, inner },
            Error::EndOfStream { record_id } => Self::EndOfStream { record_id },
            Error::SerializationError { record_id, step, inner } =>
                Self::SerializationError { record_id, step, inner },
            Error::UnknownIdentity(helper_identity) => Self::UnknownIdentity(roles.role(helper_identity)),
            Error::TooManyRecords { record_id, channel_id, total_records } =>
                Self::TooManyRecords { record_id, channel_id: ChannelId::new(roles.role(channel_id.id), channel_id.gate), total_records }
        }
    }
}

impl<M: Message> From<SendError<(usize, M)>> for Error<Role> {
    fn from(_: SendError<(usize, M)>) -> Self {
        Self::OrderedChannelError {
            inner: "ordered string".into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error<Role>>;
