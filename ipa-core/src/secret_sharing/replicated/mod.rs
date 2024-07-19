pub mod malicious;
pub mod semi_honest;

use super::{SecretSharing, SharedValue};

pub trait ReplicatedSecretSharing: SecretSharing<SharedValue = Self::V> {
    type V: SharedValue;
    fn new(a: Self::V, b: Self::V) -> Self;
    fn left(&self) -> Self::V;
    fn right(&self) -> Self::V;

    fn map<F: Fn(Self::V) -> T, R: ReplicatedSecretSharing<V = T>, T: SharedValue>(
        &self,
        f: F,
    ) -> R {
        R::new(f(self.left()), f(self.right()))
    }
}
