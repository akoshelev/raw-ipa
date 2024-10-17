use std::{
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
};

pub use Value as LabelValue;

pub const MAX_LABELS: usize = 5;

/// Dimension value (or label value) must be sendable to another thread
/// and there must be a way to show it
pub trait Value: Display + Send {
    /// Creates a unique hash for this value.
    /// It is easy to create collisions, so better avoid them,
    /// by assigning a unique integer to each value
    fn hash(&self) -> u64;

    /// Creates an owned copy of this value. Dynamic dispatch
    /// is required, because values are stored in a generic store
    /// that can't be specialized for value types.
    fn boxed(&self) -> Box<dyn LabelValue>;
}

impl LabelValue for u32 {
    fn hash(&self) -> u64 {
        u64::from(*self)
    }

    fn boxed(&self) -> Box<dyn LabelValue> {
        Box::new(*self)
    }
}

#[derive()]
pub struct Label<'lv> {
    pub name: &'static str,
    pub val: &'lv dyn Value,
}

impl Label<'_> {
    pub fn to_owned(&self) -> OwnedLabel {
        OwnedLabel {
            name: self.name,
            val: self.val.boxed(),
        }
    }
}

impl Debug for Label<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Label")
            .field("name", &self.name)
            .field("val", &format!("{}", self.val))
            .finish()
    }
}

impl Hash for Label<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.name.as_bytes());
        state.write_u64(self.val.hash());
    }
}

impl PartialEq for Label<'_> {
    fn eq(&self, other: &Self) -> bool {
        // name check should be fast - just pointer comparison.
        // val check is more involved with dynamic dispatch, so we can consider
        // making label immutable and storing a hash of the value in place
        self.name == other.name && self.val.hash() == other.val.hash()
    }
}

pub struct OwnedLabel {
    pub name: &'static str,
    pub val: Box<dyn Value>,
}

impl Clone for OwnedLabel {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            val: self.val.boxed(),
        }
    }
}

impl OwnedLabel {
    pub fn as_borrowed(&self) -> Label<'_> {
        Label {
            name: self.name,
            val: self.val.as_ref(),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn str_value(&self) -> String {
        self.val.to_string()
    }
}

impl Hash for OwnedLabel {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_borrowed().hash(state)
    }
}

impl Debug for OwnedLabel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedLabel")
            .field("name", &self.name)
            .field("val", &format!("{}", self.val))
            .finish()
    }
}

impl PartialEq for OwnedLabel {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.val.hash() == other.val.hash()
    }
}

#[cfg(test)]
mod tests {

    use crate::{key::compute_hash, metric_name};

    #[test]
    fn one_label() {
        let foo_1 = metric_name!("foo", "l1" => &1);
        let foo_2 = metric_name!("foo", "l1" => &2);

        assert_ne!(foo_1.to_owned(), foo_2);
        assert_ne!(compute_hash(&foo_1), compute_hash(&foo_2));
        assert_ne!(foo_2.to_owned(), foo_1);

        assert_eq!(compute_hash(&foo_1), compute_hash(foo_1.to_owned()))
    }

    #[test]
    #[should_panic(expected = "label names must be unique")]
    fn unique() {
        metric_name!("foo", "l1" => &1, "l1" => &0);
    }

    #[test]
    fn non_commutative() {
        assert_ne!(
            compute_hash(&metric_name!("foo", "l1" => &1, "l2" => &0)),
            compute_hash(&metric_name!("foo", "l1" => &0, "l2" => &1)),
        );
        assert_ne!(
            compute_hash(&metric_name!("foo", "l1" => &1)),
            compute_hash(&metric_name!("foo", "l1" => &1, "l2" => &1)),
        );
    }
}