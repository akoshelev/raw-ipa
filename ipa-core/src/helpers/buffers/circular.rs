use std::borrow::Borrow;
use std::num::Wrapping;
use std::ops::{Range, RangeBounds, RangeInclusive};
use std::task::Poll;
use bytes::Buf;
use generic_array::GenericArray;
use typenum::Unsigned;
use crate::ff::Serializable;
use crate::helpers::Message;

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
///
/// ## Future improvements
/// [`OrderingSender`] currently synchronizes reader and writers, but it does not have to if this
/// implementation is made thread-safe. There exists a well-known lock-free FIFO implementation
/// for a single producer, single consumer that uses atomics for read and write pointers.
/// We can't make use of it as is because there are more than one writer. However, [`OrderingSender`]
/// already knows now to allow only one write at a time, so it could be possible to make the entire
/// implementation lock-free.
///
/// [`BipBuffer`]: <https://www.codeproject.com/Articles/3479/The-Bip-Buffer-The-Circular-Buffer-with-a-Twist>
/// [`OrderingSender`]: crate::helpers::buffers::OrderingSender
pub struct CircularBuf {
    write: usize,
    read: usize,
    read_size: usize,
    write_size: usize,
    data: Vec<u8>,
}

impl CircularBuf {
    pub fn new(capacity: usize, write_size: usize, read_size: usize) -> Self {
        debug_assert!(capacity > 0 && write_size > 0 && read_size > 0,
                      "Capacity \"{capacity}\", write \"{write_size}\" and read size \"{read_size}\" must all be greater than zero"); // enforced at the level above, so debug_assert is fine
        debug_assert!(capacity % write_size == 0, "\"{}\" write size must divide capacity \"{}\"", write_size, capacity);
        Self {
            write: 0,
            read: 0,
            write_size,
            read_size,
            data: vec![0; capacity],
        }
    }

    /// todo: explain why you need to check can_write
    pub fn next(&mut self) -> Next<'_> {
        debug_assert!(self.can_write(),
            "Not enough space for the next write: only {av} bytes available, but at least {req} is required",
            av = self.free(),
            req = self.write_size
        );

        let range = self.range(self.write, self.write_size);

        Next {
            buf: self,
            range,
        }
    }

    pub fn take(&mut self) -> Vec<u8> {
        debug_assert!(!self.is_empty(), "Buffer is empty");

        // todo: explain why it works - capacity is always a multiple of write_size and read_size too.
        let delta = std::cmp::min(self.read_size, self.len());
        // todo: explain if can_read is not checked, then this may yield chunks of smaller size
        let mut ret = Vec::with_capacity(delta);
        let range = self.range(self.read, delta);

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

    pub fn len(&self) -> usize {
        if self.write >= self.read {
            self.wrap(self.write - self.read)
        } else {
            self.capacity() + self.mask(self.write) - self.mask(self.read)
        }
    }

    pub fn can_read(&self) -> bool {
        self.len() >= self.read_size
    }

    pub fn can_write(&self) -> bool {
        self.free() >= self.write_size
    }

    fn capacity(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.read == self.write
    }

    fn free(&self) -> usize {
        self.capacity() - self.len()
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

    // TODO: explain why inclusive
    fn range(&self, ptr: usize, unit: usize) -> RangeInclusive<usize> {
        self.mask(ptr)..=self.mask(ptr + unit - 1)
    }

}

/// A handle to write chunks of data directly inside [`CircularBuf`] using [`CircularBuf::next`]
/// method.
pub struct Next<'a> {
    buf: &'a mut CircularBuf,
    range: RangeInclusive<usize>
}

impl Next<'_> {

    /// Writes `M` into a slice reserved inside the [`CircularBuf`].
    ///
    /// ## Panics
    /// If the size of `M` is not equal to `write_size` of [`CircularBuf`]
    pub fn write<B: BufWriteable>(self, data: B) {
        assert_eq!(data.size(), self.buf.write_size, "Expect to keep messages of size {}, got {}", self.buf.write_size, data.size());
        data.write(&mut self.buf.data[self.range]);

        self.buf.write = self.buf.inc(self.buf.write, self.buf.write_size);
    }
}

trait BufWriteable {
    fn size(&self) -> usize;

    fn write(&self, data: &mut [u8]);
}

impl <M: Serializable> BufWriteable for &M {
    fn size(&self) -> usize {
        M::Size::USIZE
    }

    fn write(&self, data: &mut [u8]) {
        let slice = GenericArray::from_mut_slice(data);
        self.serialize(slice);
    }
}

impl BufWriteable for &[u8] {
    fn size(&self) -> usize {
        self.len()
    }

    fn write(&self, data: &mut [u8]) {
        data.copy_from_slice(self)
    }
}

#[cfg(all(test, unit_test))]
mod test {
    use std::borrow::Borrow;
    use std::convert::Infallible;
    use std::fmt::{Debug, Formatter};
    use std::marker::PhantomData;
    use std::panic;
    use std::task::Poll;
    use generic_array::GenericArray;
    use serde::Serializer;
    use typenum::{U1, U2, Unsigned};
    use crate::ff::Serializable;
    use super::CircularBuf;

    fn new_buf<B: BufSetup>() -> CircularBuf {
        CircularBuf::new(B::CAPACITY * B::UNIT_SIZE, B::UNIT_SIZE, B::READ_SIZE * B::UNIT_SIZE)
    }

    fn unwind_panic_to_str<F: FnOnce() -> CircularBuf>(f: F) -> String {
        let err = panic::catch_unwind(panic::AssertUnwindSafe(|| f())).err().unwrap();
        let err = err.downcast::<String>().unwrap();

        err.to_string()
    }

    trait BufItem: Serializable + for <'a> From<&'a usize> {}
    impl <V: Serializable + for <'a> From<&'a usize>> BufItem for V {}

    trait BufSetup {
        type Item: BufItem;

        /// The size of one element in the buffer, in bytes.
        const UNIT_SIZE: usize = <Self::Item as Serializable>::Size::USIZE;
        /// Capacity of the buffer, in units of [`UNIT_SIZE`].
        const CAPACITY: usize;
        /// Number of units written before buffer opens for reads, in units of [`UNIT_SIZE`].
        const READ_SIZE: usize;

        fn fill(buf: &mut CircularBuf) {
            for i in 0..Self::CAPACITY {
                buf.next().write(&Self::Item::from(&i));
            }
        }

        fn read_once(buf: &mut CircularBuf) -> Vec<usize> where usize: From<Self::Item> {
            assert!(!buf.is_empty(), "Buffer is empty");
            buf.take().chunks(Self::UNIT_SIZE)
                .map(|chunk| Self::Item::deserialize(GenericArray::from_slice(chunk)).unwrap())
                .map(usize::from)
                .collect()
        }
    }

    #[derive(Ord, PartialOrd, Eq, PartialEq)]
    struct TwoBytes([u8; 2]);

    impl Serializable for TwoBytes {
        type Size = U2;
        type DeserializationError = Infallible;

        fn serialize(&self, buf: &mut GenericArray<u8, Self::Size>) {
            buf[0] = self.0[0];
            buf[1] = self.0[1];
        }

        fn deserialize(buf: &GenericArray<u8, Self::Size>) -> Result<Self, Self::DeserializationError> {
            Ok(Self([buf[0], buf[1]]))
        }
    }
    
    struct FiveElements<B: BufItem = TwoBytes>(PhantomData<B>);
    impl <B: BufItem> BufSetup for FiveElements<B> {
        type Item = B;

        const UNIT_SIZE: usize = 2;
        const CAPACITY: usize = 5;
        const READ_SIZE: usize = 2;
    }

    struct One<B: BufItem = TwoBytes>(PhantomData<B>);
    impl <B: BufItem> BufSetup for One<B> {
        type Item = B;
        const CAPACITY: usize = 1;
        const READ_SIZE: usize = 1;
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
        fn iter() -> impl Iterator<Item = Self> {
            let mut count = 0;
            std::iter::repeat_with(move || {
                let next = Self::from(&count);
                count += 1;
                next
            })
        }
    }

    impl Debug for TwoBytes {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.serialize_u16(u16::from_le_bytes(self.0))
        }
    }

    #[test]
    fn empty() {
        type CircularBuf = FiveElements<TwoBytes>;

        let buf = new_buf::<CircularBuf>();
        assert_eq!(0, buf.len());
        assert_eq!(CircularBuf::CAPACITY * CircularBuf::UNIT_SIZE, buf.capacity());
        assert!(buf.can_write());
        assert!(!buf.can_read());
    }

    #[test]
    fn seq_write_read() {
        type CircularBuf = FiveElements<TwoBytes>;

        assert_ne!(0, CircularBuf::CAPACITY % CircularBuf::READ_SIZE, "This test requires buffers to be partially filled");
        assert_ne!(1, CircularBuf::CAPACITY / CircularBuf::READ_SIZE, "This test requires buffers to be partially filled");

        let mut buf = new_buf::<CircularBuf>();
        let input = (0..=CircularBuf::CAPACITY).collect::<Vec<_>>();
        let mut output = Vec::with_capacity(CircularBuf::CAPACITY);

        let mut iter = input.iter();
        while buf.can_write() {
            buf.next().write(&TwoBytes::from(iter.next().unwrap()));
        }

        assert!(!buf.can_write());
        assert!(buf.can_read());

        while buf.can_read() {
            output.extend(CircularBuf::read_once(&mut buf).into_iter().map(usize::from));
        }

        assert!(buf.can_write());
        assert!(!buf.is_empty());
        assert!(!buf.can_read());

        while (buf.len() / CircularBuf::UNIT_SIZE) < CircularBuf::READ_SIZE {
            buf.next().write(&TwoBytes::from(iter.next().unwrap()));
        }

        assert!(buf.can_write());
        output.extend(CircularBuf::read_once(&mut buf).into_iter().map(usize::from));

        assert!(buf.is_empty());
        assert_eq!(input, output);
    }

    #[test]
    fn wrap_around() {
        type CircularBuf = FiveElements<TwoBytes>;

        let mut buf = new_buf::<CircularBuf>();
        CircularBuf::fill(&mut buf);
        let _ = buf.take();

        // should be able to write more now
        while (buf.len() / CircularBuf::UNIT_SIZE) % CircularBuf::READ_SIZE != 0 {
            buf.next().write(&TwoBytes::from(&0));
        }

        while buf.can_read() {
            let _ = buf.take();
        }

        assert!(buf.is_empty());
    }

    #[test]
    fn read_size_wrap() {
        struct Six;
        impl BufSetup for Six {
            type Item = TwoBytes;
            const CAPACITY: usize = 6;
            const READ_SIZE: usize = 4;
        }

        let mut buf = new_buf::<Six>();
        Six::fill(&mut buf);

        let mut output = Vec::new();
        output.extend(Six::read_once(&mut buf).into_iter().map(usize::from));
        buf.next().write(&TwoBytes::from(&6));
        buf.next().write(&TwoBytes::from(&7));
        output.extend(Six::read_once(&mut buf).into_iter().map(usize::from));

        assert_eq!((0..8).collect::<Vec<_>>(), output);
    }

    #[test]
    fn panic_on_zero() {
        fn check_panic(capacity: usize, write: usize, read_size: usize) {
            let err =
                format!("Capacity \"{}\", write \"{}\" and read size \"{}\" must all be greater than zero", capacity, write, read_size);

            assert_eq!(err, unwind_panic_to_str(|| CircularBuf::new(capacity, write, read_size)));
        }

        check_panic(0, 0, 0);
        check_panic(2, 0, 0);
        check_panic(2, 2, 0);
    }

    #[test]
    fn panic_on_bad_write_size() {
        let capacity = 3;
        let write_size = 2;
        let err = format!("\"{write_size}\" write size must divide capacity \"{capacity}\"");

        assert_eq!(err, unwind_panic_to_str(|| CircularBuf::new(capacity, write_size, 2)));
    }

    #[test]
    fn take() {
        type CircularBuf = FiveElements<TwoBytes>;
        // take is greedy and when called is going to get whatever is available
        let mut buf = new_buf::<CircularBuf>();
        CircularBuf::fill(&mut buf);
        // can take the whole read_size chunk
        assert_eq!(vec![0, 1], CircularBuf::read_once(&mut buf));
        assert_eq!(vec![2, 3], CircularBuf::read_once(&mut buf));

        // the last item is available on demand
        assert_eq!(vec![4], CircularBuf::read_once(&mut buf));
    }

    fn test_one<T: BufSetup>() where usize: From<T::Item> {
        let mut buf = new_buf::<T>();
        T::fill(&mut buf);
        assert!(!buf.can_write());
        assert!(buf.can_read());
        assert_eq!(vec![0], T::read_once(&mut buf));
        assert!(buf.is_empty());
    }

    #[test]
    fn single_element_two_bytes() {
        test_one::<One<TwoBytes>>();
    }

    #[test]
    fn single_element_one_byte() {
        struct OneByte(u8);
        impl Serializable for OneByte {
            type Size = U1;
            type DeserializationError = Infallible;

            fn serialize(&self, buf: &mut GenericArray<u8, Self::Size>) {
                buf[0] = self.0;
            }

            fn deserialize(buf: &GenericArray<u8, Self::Size>) -> Result<Self, Self::DeserializationError> {
                Ok(Self(buf[0]))
            }
        }
        impl From<&usize> for OneByte {
            fn from(value: &usize) -> Self {
                Self(u8::try_from(*value).unwrap())
            }
        }

        impl From<OneByte> for usize {
            fn from(value: OneByte) -> Self {
                Self::from(value.0)
            }
        }

        test_one::<One<OneByte>>();
    }

    #[test]
    #[should_panic(expected = "Not enough space for the next write: only 0 bytes available, but at least 2 is required")]
    fn next_cannot_write() {
        type CircularBuf = FiveElements<TwoBytes>;

        let mut buf = new_buf::<CircularBuf>();
        CircularBuf::fill(&mut buf);

        let _ = buf.next();
    }

    #[test]
    #[should_panic(expected = "Buffer is empty")]
    fn take_cannot_read() {
        let mut buf = new_buf::<FiveElements>();
        let _ = buf.take();
    }

    #[test]
    fn small() {
        let buf = new_buf::<One<>>();
    }

    mod prop_tests {
        use std::num::Wrapping;
        use proptest::arbitrary::any;
        use proptest::{prop_compose, proptest};
        use proptest::strategy::Just;
        use proptest::test_runner::TestRunner;
        use rand::distributions::{Distribution, Standard};
        use rand::Rng;
        use rand::rngs::StdRng;
        use rand_core::SeedableRng;
        use crate::helpers::buffers::circular::CircularBuf;

        #[derive(Debug)]
        struct BufSetup {
            write_size: usize,
            read_size: usize,
            capacity: usize
        }

        prop_compose! {
            fn arb_buf(max_write_size: usize, max_units: usize)
                      (write_size in 1..max_write_size, read_units in 1..max_units)
                      (write_size in Just(write_size), read_units in Just(read_units), capacity_units in read_units..max_units)
            -> BufSetup {
                BufSetup {
                    write_size,
                    read_size: read_units * write_size,
                    capacity: capacity_units * write_size
                }
            }
        }

        impl From<BufSetup> for CircularBuf {
            fn from(value: BufSetup) -> Self {
                CircularBuf::new(value.capacity, value.write_size, value.read_size)
            }
        }

        #[derive(Debug, Eq, PartialEq)]
        enum Decision {
            Read,
            Write
        }

        impl Distribution<Decision> for Standard {
            fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Decision {
                if rng.gen() {
                    Decision::Read
                } else {
                    Decision::Write
                }
            }
        }

        fn pack(val: u8, dest_bytes: usize) -> Vec<u8> {
            let mut data = vec![0; dest_bytes];
            data[0] = val;

            data
        }

        fn take_next(buf: &mut CircularBuf, unit_size: usize) -> Vec<u8> {
            buf.take().as_slice().chunks(unit_size).map(|chunk| chunk[0]).collect()
        }

        fn read_write(setup: BufSetup, mut ops: u32, seed: u64) {
            let mut buf = CircularBuf::from(setup);
            let mut cnt = Wrapping::<u8>::default();
            let mut written = Vec::new();
            let mut read = Vec::new();
            let mut rng = StdRng::seed_from_u64(seed);
            let write_size = buf.write_size;

            for _ in 0..ops {
                if rng.gen::<Decision>() == Decision::Write && buf.can_write() {
                    buf.next().write(pack(cnt.0, write_size).as_slice());
                    written.push(cnt.0);
                    cnt += 1;
                } else if buf.can_read() {
                    read.extend(take_next(&mut buf, write_size));
                }
            }

            while !buf.is_empty() {
                read.extend(take_next(&mut buf, write_size));
            }

            assert_eq!(written, read);
        }

        proptest! {
            fn arb_read_write(setup in arb_buf(25, 99), ops in 1..1000u32, seed in any::<u64>()) {
                read_write(setup, ops, seed);
            }
        }
    }
}