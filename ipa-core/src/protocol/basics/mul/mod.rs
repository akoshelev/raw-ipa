use std::future::Future;
use async_trait::async_trait;

use crate::{
    error::Error,
    ff::Field,
    protocol::{
        context::{Context, UpgradedMaliciousContext},
        RecordId,
    },
    secret_sharing::replicated::{
        malicious::{AdditiveShare as MaliciousReplicated, ExtendableField},
        semi_honest::AdditiveShare as Replicated,
    },
};

pub(crate) mod malicious;
mod semi_honest;
pub(in crate::protocol) mod sparse;

pub use sparse::{MultiplyZeroPositions, ZeroPositions};

/// Trait to multiply secret shares. That requires communication and `multiply` function is async.
pub trait SecureMul<C: Context>: Send + Sync + Sized {
    /// Multiply and return the result of `a` * `b`.
    fn multiply<'fut, 'life0, 'life1, 'async_trait>(
        &'life0 self,
        rhs: &'life1 Self,
        ctx: C,
        record_id: RecordId,
    ) -> impl ::core::future::Future<Output = Result<Self, Error>>
            + ::core::marker::Send
            + 'async_trait
        where
            C: 'fut,
            'fut: 'async_trait,
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
    {
        self.multiply_sparse(rhs, ctx, record_id, ZeroPositions::NONE)
    }

    /// Multiply and return the result of `a` * `b`.
    /// This takes a profile of which helpers are expected to send
    /// in the form (self, left, right).
    /// This is the implementation you should invoke if you want to
    /// save work when you have sparse values.
    fn multiply_sparse<'fut, 'life0, 'life1, 'async_trait>(
        &'life0 self,
        rhs: &'life1 Self,
        ctx: C,
        record_id: RecordId,
        zeros_at: MultiplyZeroPositions,
    ) -> impl ::core::future::Future<Output = Result<Self, Error>>
            + ::core::marker::Send
            + 'async_trait
        where
            C: 'fut,
            'fut: 'async_trait,
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait;
}

use {malicious::multiply as malicious_mul, semi_honest::multiply as semi_honest_mul};

/// Implement secure multiplication for semi-honest contexts with replicated secret sharing.
impl<C: Context, F: Field> SecureMul<C> for Replicated<F> {
    fn multiply_sparse<'fut, 'life0, 'life1, 'async_trait>(
        &'life0 self,
        rhs: &'life1 Self,
        ctx: C,
        record_id: RecordId,
        zeros_at: MultiplyZeroPositions,
    ) -> impl ::core::future::Future<Output = Result<Self, Error>>
    + ::core::marker::Send
    + 'async_trait
        where
            C: 'fut,
            'fut: 'async_trait,
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait {
        semi_honest_mul(ctx, record_id, self, rhs, zeros_at)
    }
}

/// Implement secure multiplication for malicious contexts with replicated secret sharing.
impl<'a, F: ExtendableField> SecureMul<UpgradedMaliciousContext<'a, F>> for MaliciousReplicated<F> {
    fn multiply_sparse<'fut, 'life0, 'life1, 'async_trait>(
        &'life0 self,
        rhs: &'life1 Self,
        ctx: UpgradedMaliciousContext<'a, F>,
        record_id: RecordId,
        zeros_at: MultiplyZeroPositions,
    ) -> impl ::core::future::Future<Output = Result<Self, Error>> + ::core::marker::Send + 'async_trait
        where
            UpgradedMaliciousContext<'a, F>: 'fut,
            'fut: 'async_trait,
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait {
        malicious_mul(ctx, record_id, self, rhs, zeros_at)
    }
}
