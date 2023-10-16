use std::{
    cmp::Ordering::{Equal, Greater, Less},
    fmt::{Debug, Display},
    ops::{RangeInclusive, Sub},
};

use crate::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub struct Observed<T> {
    version: Arc<AtomicUsize>,
    inner: T,
}

impl<T> Observed<T> {
    pub fn get_version(&self) -> &Arc<AtomicUsize> {
        &self.version
    }

    pub fn wrap(version: &Arc<AtomicUsize>, inner: T) -> Self {
        Self {
            version: Arc::clone(version),
            inner,
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn inc_sn(&self) {
        self.version.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_sn(&self) -> usize {
        self.version.load(Ordering::Relaxed)
    }

    pub fn map<F: Fn(&Self) -> R, R>(&self, f: F) -> Observed<R> {
        Observed {
            version: Arc::clone(&self.version),
            inner: f(self),
        }
    }
}

impl<T: ObserveState> Observed<T> {
    pub fn get_state(&self) -> Option<T::State> {
        self.inner().get_state()
    }
}

pub trait ObserveState {
    type State: Debug;
    fn get_state(&self) -> Option<Self::State>;
}

impl<U> ObserveState for Vec<RangeInclusive<U>>
where
    U: Copy + Display + Eq + PartialOrd + Ord + Sub<Output = U> + From<u8>,
{
    type State = Vec<String>;
    fn get_state(&self) -> Option<Self::State> {
        Some(
            self.iter()
                .map(
                    |range| match (*range.end() - *range.start()).cmp(&U::from(1)) {
                        Less => format!("{}", range.start()),
                        Equal => format!("[{}, {}] ", range.start(), range.end()),
                        Greater => format!("[{},...,{}] ", range.start(), range.end()),
                    },
                )
                .collect(),
        )
    }
}
