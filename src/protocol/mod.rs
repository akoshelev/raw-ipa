use std::fmt::Debug;
use std::hash::Hash;

/// Defines a unique step of the IPA protocol. Step is a transformation that takes an input
/// in form of a share or set of shares and produces the secret-shared output.
///
/// Some examples of what defines a step include sorting secret shares, converting them from
/// one format to another etc.
///
/// Steps may form a hierarchy where top-level steps describe large building blocks for IPA protocol
/// (such as sort shares, convert shares, apply DP, etc) and bottom-level steps are granular enough
/// to be used to uniquely identify multiplications happening concurrently.
///
/// For testing purposes we also implement completely bogus steps that don't make much sense
/// but used to simplify testing of individual components. Those implementations are hidden behind
/// `[cfg(test)]` flag and shouldn't be considered for any purpose except unit testing.
///
/// See `IPAProtocolStep` for a canonical implementation of this trait. Every time we switch to
/// use a new circuit, there will be an additional struct/enum that implements `Step`, but eventually
/// it should converge to a single implementation.
///
/// Notes:
/// Hash supertrait will likely be removed when
/// [this issue](https://github.com/martinthomson/raw-ipa/issues/62) is fixed
pub trait Step: Copy + Clone + Debug + Eq + Hash + Send + 'static {}

/// Set of steps that define the IPA protocol.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum IPAProtocolStep {
    /// Convert from XOR shares to Replicated shares
    ConvertShares(ShareConversionStep),
    /// Sort shares by the match key
    Sort(SortStep),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ShareConversionStep {}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum SortStep {}

impl Step for IPAProtocolStep {}
