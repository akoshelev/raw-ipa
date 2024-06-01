use std::num::Wrapping;
use std::ops::{Range, RangeBounds, RangeInclusive};
use std::task::Poll;

/// This is a specialized version of circular buffer, tailored to the needs of [`OrderingSender`].
/// It behaves mostly like regular ring buffer, with a few notable differences
/// * Until it accumulates enough data as specified by `read_threshold` parameter
/// it stays closed for reads, meaning [`read`] method returns `Poll::Pending`
/// * All writes must be aligned to `read_threshold` to operate on a single heap allocation.
///
/// Internally, it is built over a [`Vec`] with two extra pointers that indicate the place to read
/// from, and to write to. When read happens, it moves the read pointer until it meets the write
/// pointer. When read points to the same location at write, this buffer is considered empty.
///
/// ## Alternative implementations
/// If alignment to `read_threshold` is too much, a [`BipBuffer`] can be used instead. I just
/// decided to implement something I am familiar with.
///
/// [`BipBuffer`]: <https://www.codeproject.com/Articles/3479/The-Bip-Buffer-The-Circular-Buffer-with-a-Twist>
pub struct CircularBuf {
    write: usize,
    read: usize,
    read_size: usize,
    write_unit: usize,
    data: Vec<u8>,
}

impl CircularBuf {
    pub fn new(capacity: usize, write_unit: usize, read_size: usize) -> Self {
        debug_assert!(capacity > 0 && write_unit > 0 && read_size > 0); // enforced at the level above, so debug_assert is fine
        debug_assert_eq!(capacity % write_unit, 0, "{} must divide capacity {}",write_unit, capacity);
        Self {
            write: 0,
            read: 0,
            write_unit,
            read_size,
            data: vec![0; capacity],
        }
    }

    fn mask(&self, val: usize) -> usize {
        val % self.data.len()
    }

    fn wrap(&self, val: usize) -> usize {
        val % (self.data.len() * 2)
    }

    fn inc(&self, val: usize, delta: usize) -> usize {
        self.wrap(val + delta)
    }

    fn range(&self, ptr: usize, unit: usize) -> RangeInclusive<usize> {
        self.mask(ptr)..=self.mask(ptr + unit - 1)
    }

    pub fn write_size(&self) -> usize {
        self.write_unit
    }

    pub fn write_slice(&mut self) -> &mut [u8] {
        debug_assert!(!self.is_full());
        let range = self.range(self.write, self.write_unit);
        // todo: explain in the docs what happens if this slice is not written into.
        self.write = self.inc(self.write, self.write_unit);

        &mut self.data[range]
    }

    fn push<R: AsRef<[u8]>>(&mut self, val: R) {
        let val = val.as_ref();
        debug_assert_eq!(self.write_unit, val.len());
        self.write_slice().copy_from_slice(val);
    }

    pub fn take(&mut self) -> Vec<u8> {
        debug_assert!(!self.is_empty());
        // todo: test for delta
        let delta = std::cmp::min(self.read_size, self.len());
        // todo: explain if can_read is not checked, then this may yield chunks of smaller size
        let mut ret = Vec::with_capacity(delta);
        let mut range = self.range(self.read, delta);

        // If the read range wraps around, we need to split it
        if range.end() < range.start() {
            ret.extend_from_slice(&self.data[*range.start()..]);
            ret.extend_from_slice(&self.data[..*range.end() + 1]);
        } else {
            ret.extend_from_slice(&self.data[range]);
        }

        self.read = self.inc(self.read, delta);

        ret
    }

    pub fn write_len(&self) -> usize {
        self.len() / self.write_unit
    }

    pub fn len(&self) -> usize {
        // self.mask(self.write.abs_diff(self.read))
        // self.wrap(self.write.wrapping_sub(self.read))
        if self.write >= self.read {
            self.wrap(self.write - self.read)
        } else {
            self.capacity() + self.mask(self.write) - self.mask(self.read)
        }
        // return N - (a - b);
    }

    pub fn free(&self) -> usize {
        self.capacity() - self.len()
    }

    pub fn is_empty(&self) -> bool {
        self.read == self.write
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.data.len()
    }

    pub fn can_read(&self) -> bool {
        self.len() >= self.read_size
    }

    pub fn capacity(&self) -> usize {
        self.data.len()
    }



// mask(val)  { return val % array.capacity; }
// wrap(val)  { return val % (array.capacity * 2); }
// inc(index) { return wrap(index + 1); }
// push(val)  { assert(!full()); array[mask(write)] = val; write = inc(write); }
// shift()    { assert(!empty()); ret = array[mask(read)]; read = inc(read); return ret; }
// empty()    { return read == write; }
// full()     { return size() == array.capacity; }
// size()     { return wrap(write - read); }
// free()     { return array.capacity - size(); }
}

#[cfg(all(test, unit_test))]
mod test {
    use std::borrow::Borrow;
    use std::fmt::{Debug, Formatter};
    use std::task::Poll;
    use serde::Serializer;
    use super::CircularBuf;

    fn new_buf<B: BufSetup>() -> CircularBuf {
        CircularBuf::new(B::CAPACITY * B::UNIT_SIZE, B::UNIT_SIZE, B::READ_SIZE * B::UNIT_SIZE)
    }

    trait BufSetup {
        const UNIT_SIZE: usize;
        /// Capacity of the buffer, in units of [`UNIT_SIZE`].
        const CAPACITY: usize;
        /// Number of units written before buffer opens for reads, in units of [`UNIT_SIZE`].
        const READ_SIZE: usize;

        fn fill(buf: &mut CircularBuf) where Self: for <'a> From<&'a usize> + AsRef<[u8]> {
            for i in 0..Self::CAPACITY {
                buf.push(&Self::from(&i).as_ref());
            }
        }

        fn read_once(buf: &mut CircularBuf) -> Vec<Self> where Self: for <'a> From<&'a [u8]> {
            assert!(buf.can_read(), "Buffer is not open for reads");
            // let Poll::Ready(bytes) = buf.re() else { unreachable!() };
            buf.take().chunks(Self::UNIT_SIZE).map(|chunk| Self::from(chunk)).collect()
        }
    }

    #[derive(Ord, PartialOrd, Eq, PartialEq)]
    struct TwoBytes([u8; 2]);

    impl BufSetup for TwoBytes {
        const UNIT_SIZE: usize = 2;
        const CAPACITY: usize = 5;
        const READ_SIZE: usize = 2;
    }

    impl AsRef<[u8]> for TwoBytes {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    impl From<&usize> for TwoBytes {
        fn from(v: &usize) -> Self {
            let v = *v;
            assert!(v <= u16::MAX as usize);
            Self([v as u8, (v >> 8) as u8])
        }
    }

    impl From<TwoBytes> for usize {
        fn from(value: TwoBytes) -> Self {
            usize::from(u16::from_le_bytes(value.0))
        }
    }

    impl TwoBytes {
        fn from_vec(v: Vec<u8>) -> Vec<Self> {
            assert_eq!(0, v.len() % Self::UNIT_SIZE);
            v.chunks(Self::UNIT_SIZE).map(|chunk|  Self([chunk[0], chunk[1]])).collect()
        }

        fn iter() -> impl Iterator<Item = Self> {
            let mut count = 0;
            std::iter::repeat_with(move || {
                let next = Self::from(&count);
                count += 1;
                next
            })
        }
    }

    impl <'a> From<&'a [u8]> for TwoBytes {
        fn from(value: &'a [u8]) -> Self {
            assert_eq!(value.len(), 2);
            Self([value[0], value[1]])
        }
    }

    impl Debug for TwoBytes {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.serialize_u16(u16::from_le_bytes(self.0))
        }
    }

    #[test]
    fn empty() {
        assert_eq!(0, new_buf::<TwoBytes>().len());
    }

    #[test]
    fn seq_write_read() {
        assert_ne!(0, TwoBytes::CAPACITY % TwoBytes::READ_SIZE, "This test requires buffers to be partially filled");
        assert_ne!(1, TwoBytes::CAPACITY / TwoBytes::READ_SIZE, "This test requires buffers to be partially filled");

        let mut buf = new_buf::<TwoBytes>();
        let input = (0..=TwoBytes::CAPACITY).collect::<Vec<_>>();
        let mut output = Vec::with_capacity(TwoBytes::CAPACITY);

        let mut iter = input.iter();
        while !buf.is_full() {
            buf.push(TwoBytes::from(iter.next().unwrap()));
        }

        assert!(buf.is_full());
        assert!(buf.can_read());

        while buf.can_read() {
            output.extend(TwoBytes::read_once(&mut buf).into_iter().map(usize::from));
        }

        assert!(!buf.is_full());
        assert!(!buf.is_empty());
        assert!(!buf.can_read());

        while buf.write_len() < TwoBytes::READ_SIZE {
            buf.push(TwoBytes::from(iter.next().unwrap()));
        }

        assert!(!buf.is_full());
        output.extend(TwoBytes::read_once(&mut buf).into_iter().map(usize::from));

        assert!(buf.is_empty());
        assert_eq!(input, output);
    }

    #[test]
    fn wrap_around() {
        let mut buf = new_buf::<TwoBytes>();
        TwoBytes::fill(&mut buf);
        let _ = buf.take();

        // should be able to write more now
        println!("before {}, {}", buf.write_len(), buf.write_len() % TwoBytes::READ_SIZE);
        while buf.write_len() % TwoBytes::READ_SIZE != 0 {
            buf.push(TwoBytes::from(&0));
        }
        println!("after {}", buf.write_len());

        while buf.can_read() {
            let _ = buf.take();
        }

        assert!(buf.is_empty());
    }
}