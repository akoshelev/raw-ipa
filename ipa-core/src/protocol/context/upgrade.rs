use std::future::Future;
use std::marker::PhantomData;

use async_trait::async_trait;
use futures::future::try_join;
use ipa_step::{Step, StepNarrow};

use crate::{
    error::Error,
    ff::Field,
    helpers::TotalRecords,
    protocol::{
        boolean::step::TwoHundredFiftySixBitOpStep, context::UpgradedContext, Gate, NoRecord,
        RecordBinding, RecordId,
    },
    secret_sharing::{
        replicated::{malicious::ExtendableField, semi_honest::AdditiveShare as Replicated},
        Linear as LinearSecretSharing,
    },
};
use crate::secret_sharing::VectorizedSecretSharing;

/// Special context type used for malicious upgrades.
///
/// The `B: RecordBinding` type parameter is used to prevent using the record ID multiple times to
/// implement an upgrade. For example, trying to use the record ID to iterate over both the inner
/// and outer vectors in a `Vec<Vec<T>>` is an error. Instead, one level of iteration can use the
/// record ID and the other can use something like a `BitOpStep`.
///
#[cfg_attr(not(descriptive_gate), doc = "```ignore")]
/// ```no_run
/// use ipa_core::protocol::{context::{UpgradeContext, UpgradeToMalicious, UpgradedMaliciousContext as C}, NoRecord, RecordId};
/// use ipa_core::ff::Fp32BitPrime as F;
/// use ipa_core::secret_sharing::replicated::{
///     malicious::AdditiveShare as MaliciousReplicated, semi_honest::AdditiveShare as Replicated,
/// };
/// // Note: Unbound upgrades only work when testing.
/// #[cfg(test)]
/// let _ = <UpgradeContext<C<'_, F>, NoRecord> as UpgradeToMalicious<Replicated<F>, _>>::upgrade;
/// let _ = <UpgradeContext<C<'_, F>, RecordId> as UpgradeToMalicious<Replicated<F>, _>>::upgrade;
/// #[cfg(test)]
/// let _ = <UpgradeContext<C<'_, F>, NoRecord> as UpgradeToMalicious<(Replicated<F>, Replicated<F>), _>>::upgrade;
/// let _ = <UpgradeContext<C<'_, F>, NoRecord> as UpgradeToMalicious<Vec<Replicated<F>>, _>>::upgrade;
/// let _ = <UpgradeContext<C<'_, F>, NoRecord> as UpgradeToMalicious<(Vec<Replicated<F>>, Vec<Replicated<F>>), _>>::upgrade;
/// ```
///
/// ```compile_fail
/// use ipa_core::protocol::{context::{UpgradeContext, UpgradeToMalicious, UpgradedMaliciousContext as C}, NoRecord, RecordId};
/// use ipa_core::ff::Fp32BitPrime as F;
/// use ipa_core::secret_sharing::replicated::{
///     malicious::AdditiveShare as MaliciousReplicated, semi_honest::AdditiveShare as Replicated,
/// };
/// // This can't be upgraded with a record-bound context because the record ID
/// // is used internally for vector indexing.
/// let _ = <UpgradeContext<C<'_, F>, RecordId> as UpgradeToMalicious<Vec<Replicated<F>>, _>>::upgrade;
pub struct UpgradeContext<C: UpgradedContext, B: RecordBinding = NoRecord> {
    ctx: C,
    record_binding: B,
}

impl<C, B> UpgradeContext<C, B>
where
    C: UpgradedContext,
    B: RecordBinding,
{
    pub fn new(ctx: C, record_binding: B) -> Self {
        Self {
            ctx,
            record_binding,
        }
    }

    fn narrow<SS: Step>(&self, step: &SS) -> Self
    where
        Gate: StepNarrow<SS>,
    {
        Self::new(self.ctx.narrow(step), self.record_binding)
    }
}


// TODO delete
pub trait UpgradeVectorizedFriendly<I: Send> {
    type UpgradeOutput: Send;

    fn upgrade_one(self, record_id: RecordId, input: I) -> impl Future<Output = Result<Self::UpgradeOutput, Error>> + Send;
}


#[async_trait]
pub trait UpgradeToMalicious<T>
where
    T: Send,
{
    type Output: Send;

    async fn upgrade(self, input: T) -> Result<Self::Output, Error>;
}

#[async_trait]
impl<C> UpgradeToMalicious<()> for UpgradeContext<C, NoRecord>
where
    C: UpgradedContext,
{
    type Output = ();
    async fn upgrade(self, _input: ()) -> Result<(), Error> {
        Ok(())
    }
}

// #[async_trait]
// impl<C, T, TM, U, UM> UpgradeToMalicious<(T, U), (TM, UM)> for UpgradeContext<C, NoRecord>
// where
//     C: UpgradedContext,
//     T: Send + 'static,
//     U: Send + 'static,
//     TM: Send + Sized + 'static,
//     UM: Send + Sized + 'static,
//     UpgradeContext<C, NoRecord>: UpgradeToMalicious<T, TM> + UpgradeToMalicious<U, UM>,
// {
//     async fn upgrade(self, input: (T, U)) -> Result<(TM, UM), Error> {
//         try_join(
//             self.narrow(&TwoHundredFiftySixBitOpStep::from(0))
//                 .upgrade(input.0),
//             self.narrow(&TwoHundredFiftySixBitOpStep::from(1))
//                 .upgrade(input.1),
//         )
//         .await
//     }
// }

#[async_trait]
impl<C, I, M> UpgradeToMalicious<Vec<I>> for UpgradeContext<C, NoRecord>
where
    C: UpgradedContext,
    // I: IntoIterator + Send + 'static,
    // I::IntoIter: ExactSizeIterator + Send,
    I: Send + 'static,
    M: Send + 'static,
    UpgradeContext<C, RecordId>: UpgradeToMalicious<I, Output = M>,
{
    type Output = Vec<M>;

    async fn upgrade(self, input: Vec<I>) -> Result<Vec<M>, Error> {
        let iter = input.into_iter();
        let ctx = self
            .ctx
            .set_total_records(TotalRecords::specified(iter.len())?);
        let ctx_ref = &ctx;
        ctx.try_join(iter.enumerate().map(|(i, share)| async move {
            // TODO: make it a bit more ergonomic to call with record id bound
            UpgradeContext::new(ctx_ref.clone(), RecordId::from(i))
                .upgrade(share)
                .await
        }))
        .await
    }
}

#[async_trait]
impl<C, F> UpgradeToMalicious<Replicated<F>> for UpgradeContext<C, RecordId>
where
    C: UpgradedContext<Field = F>,
    F: ExtendableField,
{
    type Output = C::Share;
    async fn upgrade(self, input: Replicated<F>) -> Result<C::Share, Error> {
        self.ctx.upgrade_one(self.record_binding, input).await
    }
}

#[cfg(test)]
#[async_trait]
impl<C, F> UpgradeToMalicious<(Replicated<F>, Replicated<F>)> for UpgradeContext<C, RecordId>
where
    C: UpgradedContext<Field = F>,
    F: ExtendableField,
{
    type Output = (C::Share, C::Share);
    async fn upgrade(self, (l, r): (Replicated<F>, Replicated<F>)) -> Result<(C::Share, C::Share), Error> {
        let l_ctx = self.ctx.narrow("left_upgrade");
        let r_ctx = self.ctx.narrow("right_upgrade");
        try_join(
            l_ctx.upgrade_one(self.record_binding, l),
            r_ctx.upgrade_one(self.record_binding, r)
        ).await
    }
}

#[cfg(test)]
#[async_trait]
impl<C, F, M> UpgradeToMalicious<(Replicated<F>, Replicated<F>)> for UpgradeContext<C, NoRecord>
where
    C: UpgradedContext<Field = F>,
    F: ExtendableField,
    M: Send + 'static,
    UpgradeContext<C, RecordId>: UpgradeToMalicious<Replicated<F>, Output = M>,
{
    type Output = (M, M);

    async fn upgrade(self, (l, r): (Replicated<F>, Replicated<F>)) -> Result<Self::Output, Error> {
        let ctx = if self.ctx.total_records().is_specified() {
            self.ctx
        } else {
            self.ctx.set_total_records(TotalRecords::ONE)
        };

        // UpgradeContext::new(ctx, RecordId::FIRST)
        //     .upgrade(input)
        //     .await

        let l_ctx = ctx.narrow("left_upgrade");
        let r_ctx = ctx.narrow("right_upgrade");

        try_join(UpgradeContext::new(l_ctx, RecordId::FIRST)
            .upgrade(l),
        UpgradeContext::new(r_ctx, RecordId::FIRST)
            .upgrade(r)
        ).await
    }
}

// Impl to upgrade a single `Replicated<F>` using a non-record-bound context. Used for tests.
#[cfg(test)]
#[async_trait]
impl<C, F, M> UpgradeToMalicious<Replicated<F>> for UpgradeContext<C, NoRecord>
where
    C: UpgradedContext,
    F: ExtendableField,
    M: Send + 'static,
    UpgradeContext<C, RecordId>: UpgradeToMalicious<Replicated<F>, Output = M>,
{
    type Output = M;
    async fn upgrade(self, input: Replicated<F>) -> Result<M, Error> {
        let ctx = if self.ctx.total_records().is_specified() {
            self.ctx
        } else {
            self.ctx.set_total_records(TotalRecords::ONE)
        };
        UpgradeContext::new(ctx, RecordId::FIRST)
            .upgrade(input)
            .await
    }
}
