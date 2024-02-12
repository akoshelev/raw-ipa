use generic_array::GenericArray;
use typenum::U1;
use crate::protocol::prss::FromRandom;


/// TODO: shard index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId(u32);


impl From<u32> for ShardId {
    fn from(value: u32) -> Self {
        ShardId(value)
    }
}

impl From<ShardId> for usize {
    fn from(value: ShardId) -> Self {
        // FIXME: static assertions
        usize::try_from(value.0).unwrap()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardCount(u32);

impl From<u32> for ShardCount {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<ShardCount> for u128 {
    fn from(value: ShardCount) -> Self {
        Self::from(value.0)
    }
}

impl From<ShardCount> for usize {
    fn from(value: ShardCount) -> Self {
        // FIXME
        Self::try_from(value.0).unwrap()
    }
}

impl ShardCount {
    pub const ONE: Self = ShardCount(1);

    pub fn iter(&self) -> impl Iterator<Item = ShardId> {
        (0..self.0).into_iter().map(ShardId)
    }
}

pub trait ShardConfiguration {
    fn my_shard(&self) -> ShardId;

    fn shard_count(&self) -> ShardCount;
}