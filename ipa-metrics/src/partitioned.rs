use std::cell::Cell;
use hashbrown::hash_map::Entry;
use rustc_hash::FxBuildHasher;
use crate::kind::CounterValue;
use crate::MetricName;
use crate::store::Store;

/// Each partition is a unique 16 byte value.
type Partition = u128;

pub(super) fn set_partition(new: Partition) {
    PARTITION.set(Some(new));
}

fn current_partition() -> Option<Partition> {
    PARTITION.get()
}

thread_local! {
    static PARTITION: Cell<Option<Partition>> = Cell::new(None);
}


/// Provides the same functionality as [`Store`], but partitioned
/// across many dimensions. There is an extra price for it, so
/// don't use it, unless you need it.
/// The dimension is set through [`std::thread::LocalKey`], so
/// each thread can set only one dimension at a time.
///
/// For safety, it will panic if partition is not set. If partitioning
/// is not required, turn off the `partitions` feature and
/// use [`Store`] instead
pub struct PartitionedStore {
    inner: hashbrown::HashMap<Partition, Store, FxBuildHasher>,
}

impl Default for PartitionedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionedStore {
    pub const fn new() -> Self {
        Self {
            inner: hashbrown::HashMap::with_hasher(FxBuildHasher),
        }
    }

    pub fn current_mut(&mut self) -> &mut Store {
        self.get_mut(current_partition())
    }

    pub fn with_current_partition<F: FnOnce(&mut Store) -> T, T>(&mut self, f: F) -> T {
        let mut store = self.get_mut(current_partition());
        f(&mut store)
    }

    pub fn with_partition<F: FnOnce(&mut Store) -> T, T>(&mut self, partition: Partition, f: F) -> T {
        let mut store = self.get_mut(Some(partition));
        f(&mut store)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn merge(&mut self, other: Self) {
        for (partition, store) in other.inner {
            self.get_mut(Some(partition)).merge(store);
        }
    }

    pub fn counter_value(&self, name: &MetricName) -> CounterValue {
        if let Some(partition) = current_partition() {
            self.inner.get(&partition).map(|store| store.counter_value(name)).unwrap_or_default()
        } else {
            CounterValue::default()
        }
    }

    fn get_mut(&mut self, partition: Option<u128>) -> &mut Store {
        if let Some(v) = partition {
            match self.inner.entry(v) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(Store::default()),
            }
        } else {
            panic!("Partition must be set before PartitionedStore can be used.")
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::metric_name;
    use crate::partitioned::{set_partition, PartitionedStore};

    #[test]
    fn unique_partition() {
        let metric = metric_name!("foo");
        let mut store = PartitionedStore::new();
        store.with_partition(1, |store| {
            store.counter(&metric).inc(1);
        });
        store.with_partition(5, |store| {
            store.counter(&metric).inc(5);
        });

        assert_eq!(5, store.with_partition(5, |store| store.counter(&metric).get()));
        assert_eq!(1, store.with_partition(1, |store| store.counter(&metric).get()));
        assert_eq!(0, store.with_partition(10, |store| store.counter(&metric).get()));
    }

    #[test]
    #[should_panic(expected = "Partition must be set before PartitionedStore can be used.")]
    fn current_panic() {
        let mut store = PartitionedStore::new();
        store.with_current_partition(|_| unreachable!());
    }

    #[test]
    fn current_partition() {
        let metric = metric_name!("foo");
        let mut store = PartitionedStore::new();
        set_partition(4);

        store.with_current_partition(|store| {
            store.counter(&metric).inc(1);
        });
        store.with_current_partition(|store| {
            store.counter(&metric).inc(5);
        });

        assert_eq!(6, store.with_current_partition(|store| store.counter(&metric).get()));
    }
}