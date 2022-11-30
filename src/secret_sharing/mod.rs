mod malicious_replicated;
mod replicated;
mod xor;

use crate::ff::Field;
pub(crate) use malicious_replicated::ThisCodeIsAuthorizedToDowngradeFromMalicious;
pub use malicious_replicated::{Downgrade as DowngradeMalicious, MaliciousReplicated};
pub use replicated::Replicated;
use std::fmt::Debug;
use std::ops::{Add, AddAssign, Mul, Neg, Sub, SubAssign};
pub use xor::XorReplicated;

/// Secret share of a secret has additive and multiplicative properties.
pub trait SecretSharing<F>:
    for<'a> Add<&'a Self, Output = Self>
    + for<'a> AddAssign<&'a Self>
    + Neg
    + for<'a> Sub<&'a Self, Output = Self>
    + for<'a> SubAssign<&'a Self>
    + Mul<F>
    + Clone
    + Debug
    + Default
    + Sized
    + Sync
{
}

impl<F: Field> SecretSharing<F> for Replicated<F> {}
impl<F: Field> SecretSharing<F> for MaliciousReplicated<F> {}
