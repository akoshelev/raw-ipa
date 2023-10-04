use generic_array::GenericArray;
use curve25519_dalek::{ristretto::{CompressedRistretto, RistrettoPoint},constants, Scalar};
use typenum::U32;
use sha2::Sha256;
use hkdf::Hkdf;


use crate::{
    ff::{Serializable,ec_prime_field::Fp25519},
    secret_sharing::{Block, SharedValue},
};

impl Block for CompressedRistretto {
    type Size = U32;
}

///ristretto point for curve 25519
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct RP25519(<Self as SharedValue>::Storage);

//how to add const ONE: Self = Self(Scalar::ONE); within the struct, do I need to do it via a trait?

/// using compressed ristretto point, Zero is generator of the curve, i.e. g^0
impl SharedValue for RP25519 {
    type Storage = CompressedRistretto;
    const BITS: u32 = 256;
    const ZERO: Self = Self(constants::RISTRETTO_BASEPOINT_COMPRESSED);
}

impl Serializable for RP25519 {
    type Size = <<RP25519 as SharedValue>::Storage as Block>::Size;

    fn serialize(&self, buf: &mut GenericArray<u8, Self::Size>) {
        let raw = &self.0.as_bytes()[..buf.len()] ;
        buf.copy_from_slice(raw);
    }

    fn deserialize(buf: &GenericArray<u8, Self::Size>) -> Self {
        RP25519(CompressedRistretto::from_slice(buf).unwrap())
    }
}


impl rand::distributions::Distribution<RP25519> for rand::distributions::Standard {
    fn sample<R: crate::rand::Rng + ?Sized>(&self, rng: &mut R) -> RP25519 {
        //Fp25519(Scalar::random(rng: &mut R))
        let mut scalar_bytes = [0u8; 64];
        rng.fill_bytes(&mut scalar_bytes);
        RP25519(RistrettoPoint::from_uniform_bytes(&scalar_bytes).compress())
    }
}


impl std::ops::Add for RP25519 {
type Output = Self;

fn add(self, rhs: Self) -> Self::Output {
    Self((self.0.decompress().unwrap()+rhs.0.decompress().unwrap()).compress())
}
}

impl std::ops::AddAssign for RP25519 {
#[allow(clippy::assign_op_pattern)]
fn add_assign(&mut self, rhs: Self) {
    *self = *self + rhs;
}
}

impl std::ops::Neg for RP25519 {
type Output = Self;

fn neg(self) -> Self::Output {
    Self(self.0.decompress().unwrap().neg().compress())
}
}

impl std::ops::Sub for RP25519 {
type Output = Self;

fn sub(self, rhs: Self) -> Self::Output {
    Self((self.0.decompress().unwrap()-rhs.0.decompress().unwrap()).compress())
}
}

impl std::ops::SubAssign for RP25519 {
#[allow(clippy::assign_op_pattern)]
fn sub_assign(&mut self, rhs: Self) {
    *self = *self - rhs;
}
}


///Scalar Multiplication
///<'a, 'b> std::ops::Mul<&'b Fp25519> for &'a
impl RP25519 {

fn s_mul(self, rhs: Fp25519) -> RP25519 {
    RP25519((self.0.decompress().unwrap() * Scalar::from(rhs)).compress())
}
}


///do not use
impl std::ops::Mul for RP25519 {
    type Output = Self;

    fn mul(self, _rhs: RP25519) -> Self::Output {
        panic!("Two curve points cannot be multiplied! Do not use *, *= for RP25519 or secret shares of RP25519");
    }
}

///do not use
impl std::ops::MulAssign for RP25519 {

    fn mul_assign(& mut self, _rhs: RP25519)  {
        panic!("Two curve points cannot be multiplied! Do not use *, *= for RP25519 or secret shares of RP25519");
    }
}

impl From<Scalar> for RP25519 {
    fn from(s: Scalar) -> Self {
        RP25519(RistrettoPoint::mul_base(&s).compress())
    }
}

impl From<Fp25519> for RP25519 {
    fn from(s: Fp25519) -> Self {
        RP25519(RistrettoPoint::mul_base(&s.into()).compress())
    }
}

macro_rules! cp_hash_impl {
    ( $u_type:ty, $byte_size:literal) => {
        impl From<RP25519> for $u_type {
            fn from(s: RP25519) -> Self {
                let hk = Hkdf::<Sha256>::new(None, s.0.as_bytes());
                let mut okm = [0u8; $byte_size];
                //error invalid length from expand only happens when okm is very large
                hk.expand(&[], &mut okm).unwrap();
                <$u_type>::from_le_bytes(okm)
            }
        }
    }
}

cp_hash_impl!(
    u128,
    16
);

cp_hash_impl!(
    u64,
    8
);

cp_hash_impl!(
    u32,
    4
);



#[cfg(all(test, unit_test))]
mod test {
    use generic_array::GenericArray;
    use crate::ff::curve_points::RP25519;
    use crate::ff::Serializable;
    use typenum::U32;
    use curve25519_dalek::scalar::Scalar;
    use rand::{thread_rng, Rng};
    use crate::ff::ec_prime_field::Fp25519;
    use crate::secret_sharing::SharedValue;

    #[test]
    fn serde_25519() {
        let input:[u8;32] = [
            0x01, 0xff,0x00, 0xff,0x00, 0xff,0x00, 0xff,
            0x00, 0xff,0x00, 0xff,0x00, 0xff,0x00, 0xff,
            0x00, 0xff,0x00, 0xff,0x00, 0xff,0x00, 0xff,
            0x00, 0xff,0x00, 0xff,0x00, 0x00,0x00, 0x00
        ];
        let mut output: GenericArray<u8,U32> = [0u8;32].into();
        let a = RP25519::deserialize(&input.into());
        assert_eq!(a.0.as_bytes()[..32],input);
        a.serialize(&mut output);
        assert_eq!(a.0.as_bytes()[..32],output.as_slice()[..32]);
        assert_eq!(input,output.as_slice()[..32]);
    }

    #[test]
    fn scalar_to_point() {
        let a = Scalar::ONE;
        let b : RP25519 = a.clone().into();
        let d : Fp25519 = a.into();
        let c : RP25519 = RP25519::from(d);
        assert_eq!(b,RP25519::ZERO);
        assert_eq!(c,RP25519::ZERO);
    }

    #[test]
    fn curve_arithmetics() {
        let mut rng = thread_rng();
        let a = rng.gen::<Fp25519>();
        let b = rng.gen::<Fp25519>();
        let c = a+b;
        let d = RP25519::from(a)+RP25519::from(b);
        assert_eq!(d, RP25519::from(c));
        assert_ne!(d, RP25519::ZERO);
        let e = rng.gen::<Fp25519>();
        let f=rng.gen::<Fp25519>();
        let g =e*f;
        let h = RP25519::from(e).s_mul(f);
        assert_eq!(h,RP25519::from(g));
        assert_ne!(h, RP25519::ZERO);
    }

    #[test]
    fn curve_point_to_hash() {
        let mut rng = thread_rng();
        let a = rng.gen::<RP25519>();
        assert_ne!(0u128,u128::from(a));
        assert_ne!(0u64,u64::from(a));
        assert_ne!(0u32,u32::from(a));
    }

}