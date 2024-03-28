#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod sum;
#[cfg(feature = "build")]
mod track;
mod variant;

use std::env;

use ipa_step::{name::GateName, COMPACT_GATE_INCLUDE_ENV};
use proc_macro::TokenStream as TokenStreamBasic;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Ident};
use variant::Generator;

use crate::variant::VariantAttribute;

/// A utility trait that allows for more streamlined error reporting.
trait IntoSpan {
    fn into_span(self) -> syn::Result<Span>;

    fn error<T>(self, msg: &str) -> syn::Result<T>
    where
        Self: Sized,
    {
        Err(self.raw_err(msg))
    }

    fn raw_err(self, msg: &str) -> syn::Error
    where
        Self: Sized,
    {
        match self.into_span() {
            Ok(span) => syn::Error::new(span, msg),
            Err(e) => e,
        }
    }
}

impl IntoSpan for Span {
    fn into_span(self) -> syn::Result<Span> {
        Ok(self)
    }
}

impl<T> IntoSpan for &T
where
    T: syn::spanned::Spanned,
{
    fn into_span(self) -> syn::Result<Span> {
        Ok(self.span())
    }
}

fn wrap_impl(res: Result<TokenStream, syn::Error>) -> TokenStreamBasic {
    TokenStreamBasic::from(match res {
        Ok(s) => s,
        Err(e) => e.into_compile_error(),
    })
}

/// Derive an implementation of `Step` and `CompactStep`.
///
/// # Panics
/// This can fail in a bunch of ways.
/// * The derive attribute needs to be used on a enum.
/// * Attributes need to be set correctly.
#[proc_macro_derive(CompactStep, attributes(step))]
pub fn derive_step(input: TokenStreamBasic) -> TokenStreamBasic {
    wrap_impl(derive_step_impl(&parse_macro_input!(input as DeriveInput)))
}

/// Generate a `Gate` implementation from an implementation of `CompactStep`.
/// The resulting object will be the top-level entry-point for a complete protocol.
#[proc_macro_derive(CompactGate)]
pub fn derive_gate(input: TokenStreamBasic) -> TokenStreamBasic {
    TokenStreamBasic::from(derive_gate_impl(&parse_macro_input!(input as DeriveInput)))
}

/// Used to generate a map of steps for use in a build script.
#[cfg(feature = "build")]
#[proc_macro]
pub fn track_steps(input: TokenStreamBasic) -> TokenStreamBasic {
    wrap_impl(track::track_steps_impl(TokenStream::from(input)))
}

fn derive_step_impl(ast: &DeriveInput) -> Result<TokenStream, syn::Error> {
    let ident = &ast.ident;
    let mut g = Generator::default();
    let attr = match &ast.data {
        Data::Enum(data) => {
            for v in VariantAttribute::parse_variants(data)? {
                g.add_variant(&v);
            }
            VariantAttribute::parse_outer(ident, &ast.attrs, None)?
        }
        Data::Struct(data) => VariantAttribute::parse_outer(ident, &ast.attrs, Some(&data.fields))?,
        Data::Union(..) => {
            return ast
                .ident
                .error("Step can only be derived for a struct or enum")
        }
    };
    Ok(g.generate(ident, &attr))
}

fn derive_gate_impl(ast: &DeriveInput) -> TokenStream {
    let step = ast.ident.to_string();
    let gate_name = GateName::new(&step);
    let name = Ident::new(&gate_name.name(), Span::call_site());

    let mut result = quote! {
        /// A compact `Gate` corresponding to #step.
        ///
        /// Note that the ordering of this gate implementation might not match
        /// the ordering of [`Descriptive`].
        ///
        /// [`Descriptive`]: crate::descriptive::Descriptive
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct #name(::ipa_step::CompactGateIndex);
        impl ::ipa_step::Gate for #name {}
        impl ::std::default::Default for #name {
            fn default() -> Self {
                Self(0)
            }
        }

        impl ::std::fmt::Display for #name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_ref())
            }
        }
        impl ::std::fmt::Debug for #name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str("gate=")?;
                <Self as ::std::fmt::Display>::fmt(self, f)
            }
        }
    };

    // This environment variable is set by build scripts,
    // and is then available (only) during the main build.
    if env::var(COMPACT_GATE_INCLUDE_ENV).is_ok() {
        let filename = gate_name.filename();
        result.extend(quote! {
            include!(concat!(env!("OUT_DIR"), "/", #filename));
        });
    } else {
        result.extend(quote! {
        impl ::std::convert::AsRef<str> for #name {
            fn as_ref(&self) -> &str {
                unimplemented!()
            }
        }

        impl ::std::convert::From<&str> for #name {
            fn from(s: &str) -> Self {
                unimplemented!()
            }
        }
        });
    }

    result
}

#[cfg(test)]
mod test {
    use proc_macro2::TokenStream;
    use quote::quote;
    use syn::parse2;

    use super::derive_step_impl;

    fn derive(input: TokenStream) -> Result<TokenStream, TokenStream> {
        match syn::parse2::<syn::DeriveInput>(input) {
            Ok(di) => derive_step_impl(&di),
            Err(e) => Err(e),
        }
        .map_err(syn::Error::into_compile_error)
    }

    fn pretty(tokens: TokenStream) -> String {
        prettyplease::unparse(&parse2(tokens).unwrap())
    }

    fn derive_success(input: TokenStream, expected: &TokenStream) {
        let output = derive(input).unwrap();
        assert_eq!(
            output.to_string(),
            expected.to_string(),
            "Got:\n{p}",
            p = pretty(output),
        );
    }

    fn derive_failure(input: TokenStream, msg: &str) {
        let expected = quote! { ::core::compile_error!{ #msg } };
        assert_eq!(derive(input).unwrap_err().to_string(), expected.to_string());
    }

    #[test]
    fn simple() {
        let code = derive(quote! {
            #[derive(CompactStep)]
            enum Simple {
                Arm,
                #[step(count = 3)]
                Leg(usize),
            }
        })
        .unwrap();

        println!("{code}");
        assert!(syn::parse2::<syn::File>(code).is_ok());
    }

    #[test]
    fn empty() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                enum EmptyEnum {}
            },
            &quote! {
                impl ::ipa_step::Step for EmptyEnum {}

                impl ::std::convert::AsRef<str> for EmptyEnum {
                    fn as_ref(&self) -> &str {
                        "empty_enum"
                    }
                }

                impl ::ipa_step::CompactStep for EmptyEnum {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = 1;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex { 0 }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i == 0 => String::from("empty_enum"),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn empty_named() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                #[step(name = "empty")]
                enum EmptyEnum {}
            },
            &quote! {
                impl ::ipa_step::Step for EmptyEnum {}

                impl ::std::convert::AsRef<str> for EmptyEnum {
                    fn as_ref(&self) -> &str {
                        "empty"
                    }
                }

                impl ::ipa_step::CompactStep for EmptyEnum {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = 1;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex { 0 }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i == 0 => String::from("empty"),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn one_armed() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                enum OneArm {
                    Arm,
                }
            },
            &quote! {
                impl ::ipa_step::Step for OneArm {}

                impl ::std::convert::AsRef<str> for OneArm {
                    fn as_ref(&self) -> &str {
                        match self {
                            Self::Arm => "arm",
                        }
                    }
                }

                impl ::ipa_step::CompactStep for OneArm {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = 1;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex {
                        match self {
                            Self::Arm => 0,
                        }
                    }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i == 0 => Self::Arm.as_ref().to_owned(),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn one_armed_named() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                enum OneArm {
                    #[step(name = "a")]
                    Arm,
                }
            },
            &quote! {
                impl ::ipa_step::Step for OneArm {}

                impl ::std::convert::AsRef<str> for OneArm {
                    fn as_ref(&self) -> &str {
                        match self {
                            Self::Arm => "a",
                        }
                    }
                }

                impl ::ipa_step::CompactStep for OneArm {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = 1;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex {
                        match self {
                            Self::Arm => 0,
                        }
                    }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i == 0 => Self::Arm.as_ref().to_owned(),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn int_arm() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                enum ManyArms {
                    #[step(count = 3)]
                    Arm(u8),
                }
            },
            &quote! {
                impl ::ipa_step::Step for ManyArms {}

                #[allow(
                    clippy::useless_conversion,
                    clippy::unnecessary_fallible_conversions,
                )]
                impl ::std::convert::AsRef<str> for ManyArms {
                    fn as_ref(&self) -> &str {
                        const ARM_NAMES: [&str; 3] = ["arm0", "arm1", "arm2"];
                        match self {
                            Self::Arm(i) => ARM_NAMES[usize::try_from(*i).unwrap()],
                        }
                    }
                }

                #[allow(
                    clippy::useless_conversion,
                    clippy::unnecessary_fallible_conversions,
                    clippy::identity_op,
                )]
                impl ::ipa_step::CompactStep for ManyArms {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = 3;
                    fn base_index (& self) -> ::ipa_step::CompactGateIndex {
                        match self {
                            Self::Arm (i) => ::ipa_step::CompactGateIndex::try_from(*i).unwrap(),
                        }
                    }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i < 3 => Self::Arm(u8::try_from(i - (0)).unwrap()).as_ref().to_owned(),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn int_arm_named() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                enum ManyArms {
                    #[step(count = 3, name = "a")]
                    Arm(u8),
                }
            },
            &quote! {
                impl ::ipa_step::Step for ManyArms {}

                #[allow(
                    clippy::useless_conversion,
                    clippy::unnecessary_fallible_conversions,
                )]
                impl ::std::convert::AsRef<str> for ManyArms {
                    fn as_ref(&self) -> &str {
                        const ARM_NAMES: [&str; 3] = ["a0", "a1", "a2"];
                        match self {
                            Self::Arm(i) => ARM_NAMES[usize::try_from(*i).unwrap()],
                        }
                    }
                }

                #[allow(
                    clippy::useless_conversion,
                    clippy::unnecessary_fallible_conversions,
                    clippy::identity_op,
                )]
                impl ::ipa_step::CompactStep for ManyArms {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = 3;
                    fn base_index (& self) -> ::ipa_step::CompactGateIndex {
                        match self {
                            Self::Arm (i) => ::ipa_step::CompactGateIndex::try_from(*i).unwrap(),
                        }
                    }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i < 3 => Self::Arm(u8::try_from(i - (0)).unwrap()).as_ref().to_owned(),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn child_arm() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                enum Parent {
                    #[step(child = Child)]
                    Offspring,
                }
            },
            &quote! {
                impl ::ipa_step::Step for Parent {}

                impl ::std::convert::AsRef<str> for Parent {
                    fn as_ref(&self) -> &str {
                        match self {
                            Self::Offspring => "offspring",
                        }
                    }
                }

                impl ::ipa_step::CompactStep for Parent {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = <Child as ::ipa_step::CompactStep>::STEP_COUNT + 1;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex {
                        match self {
                            Self::Offspring => 0,
                        }
                    }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i == 0 => Self::Offspring.as_ref().to_owned(),
                            _ if i < <Child as ::ipa_step::CompactStep>::STEP_COUNT + 1
                                => Self::Offspring.as_ref().to_owned() + "/" + &<Child as ::ipa_step::CompactStep>::step_string(i - (1)),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }

                    fn step_narrow_type(i: ::ipa_step::CompactGateIndex) -> Option<&'static str> {
                        match i {
                            _ if i == 0 => Some(::std::any::type_name::<Child>()),
                            _ if (1..<Child as ::ipa_step::CompactStep>::STEP_COUNT + 1).contains(&i)
                              => <Child as ::ipa_step::CompactStep>::step_narrow_type(i - (1)),
                            _ => None,
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn child_arm_named() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                enum Parent {
                    #[step(child = Child, name = "spawn")]
                    Offspring,
                }
            },
            &quote! {
                impl ::ipa_step::Step for Parent {}

                impl ::std::convert::AsRef<str> for Parent {
                    fn as_ref(&self) -> &str {
                        match self {
                            Self::Offspring => "spawn",
                        }
                    }
                }

                impl ::ipa_step::CompactStep for Parent {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = <Child as ::ipa_step::CompactStep>::STEP_COUNT + 1;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex {
                        match self {
                            Self::Offspring => 0,
                        }
                    }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i == 0 => Self::Offspring.as_ref().to_owned(),
                            _ if i < <Child as ::ipa_step::CompactStep>::STEP_COUNT + 1
                                => Self::Offspring.as_ref().to_owned() + "/" + &<Child as ::ipa_step::CompactStep>::step_string(i - (1)),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }

                    fn step_narrow_type(i: ::ipa_step::CompactGateIndex) -> Option<&'static str> {
                        match i {
                            _ if i == 0 => Some(::std::any::type_name::<Child>()),
                            _ if (1..<Child as ::ipa_step::CompactStep>::STEP_COUNT + 1).contains(&i)
                              => <Child as ::ipa_step::CompactStep>::step_narrow_type(i - (1)),
                            _ => None,
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn empty_child() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                #[step(child = Child)]
                enum Parent {}
            },
            &quote! {
                impl ::ipa_step::Step for Parent {}

                impl ::std::convert::AsRef<str> for Parent {
                    fn as_ref(&self) -> &str {
                        "parent"
                    }
                }

                impl ::ipa_step::CompactStep for Parent {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = <Child as ::ipa_step::CompactStep>::STEP_COUNT + 1;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex { 0 }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i == 0 => String::from("parent"),
                            _ if i < <Child as ::ipa_step::CompactStep>::STEP_COUNT + 1
                                => String::from("parent") + "/" + &<Child as ::ipa_step::CompactStep>::step_string(i - (1)),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }

                    fn step_narrow_type(i: ::ipa_step::CompactGateIndex) -> Option<&'static str> {
                        match i {
                            _ if i == 0 => Some(::std::any::type_name::<Child>()),
                            _ if (1..<Child as ::ipa_step::CompactStep>::STEP_COUNT + 1).contains(&i)
                              => <Child as ::ipa_step::CompactStep>::step_narrow_type(i - (1)),
                            _ => None,
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn int_child() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                enum Parent {
                    #[step(child = Child, count = 5, name = "spawn")]
                    Offspring(u8),
                }
            },
            &quote! {
                impl ::ipa_step::Step for Parent {}


                #[allow(
                    clippy::useless_conversion,
                    clippy::unnecessary_fallible_conversions,
                )]
                impl ::std::convert::AsRef<str> for Parent {
                    fn as_ref(&self) -> &str {
                        const OFFSPRING_NAMES: [&str; 5] =
                            ["spawn0", "spawn1", "spawn2", "spawn3", "spawn4"];
                        match self {
                            Self::Offspring(i) => OFFSPRING_NAMES[usize::try_from(*i).unwrap()],
                        }
                    }
                }


                #[allow(
                    clippy::useless_conversion,
                    clippy::unnecessary_fallible_conversions,
                    clippy::identity_op,
                )]
                impl ::ipa_step::CompactStep for Parent {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = (<Child as ::ipa_step::CompactStep>::STEP_COUNT + 1) * 5;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex {
                        match self {
                            Self::Offspring(i) => (<Child as ::ipa_step::CompactStep>::STEP_COUNT + 1) * ::ipa_step::CompactGateIndex::try_from(*i).unwrap(),
                        }
                    }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i < (<Child as ::ipa_step::CompactStep>::STEP_COUNT + 1) * 5 => {
                                let offset = i - (0);
                                let divisor = <Child as ::ipa_step::CompactStep>::STEP_COUNT + 1;
                                let s = Self::Offspring(u8::try_from(offset / divisor).unwrap())
                                    .as_ref()
                                    .to_owned();
                                if let Some(v) = (offset % divisor).checked_sub(1) {
                                    s + "/" + &<Child as ::ipa_step::CompactStep>::step_string(v)
                                } else {
                                    s
                                }
                            }
                            _ => panic!(
                                "step {i} is not valid for {t}",
                                t = ::std::any::type_name::<Self>()
                            ),
                        }
                    }
                    fn step_narrow_type(i: ::ipa_step::CompactGateIndex) -> Option<&'static str> {
                        match i {
                            _ if (0..(<Child as ::ipa_step::CompactStep>::STEP_COUNT + 1) * 5).contains(&i) => {
                                let offset = i - (0);
                                let divisor = <Child as ::ipa_step::CompactStep>::STEP_COUNT + 1;
                                if let Some(v) = (offset % divisor).checked_sub(1) {
                                    <Child as ::ipa_step::CompactStep>::step_narrow_type(v)
                                } else {
                                    Some(::std::any::type_name::<Child>())
                                }
                            }
                            _ => None,
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn all_arms() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                enum AllArms {
                    Empty,
                    #[step(count = 3)]
                    Int(usize),
                    #[step(child = ::some::other::StepEnum)]
                    Child,
                    Final,
                }
            },
            &quote! {
                impl ::ipa_step::Step for AllArms {}

                #[allow(
                    clippy::useless_conversion,
                    clippy::unnecessary_fallible_conversions,
                )]
                impl ::std::convert::AsRef<str> for AllArms {
                    fn as_ref(&self) -> &str {
                        const INT_NAMES: [&str; 3] = ["int0", "int1", "int2"];
                        match self {
                            Self::Empty => "empty",
                            Self::Int(i) => INT_NAMES[usize::try_from(*i).unwrap()],
                            Self::Child => "child",
                            Self::Final => "final",
                        }
                    }
                }

                #[allow(
                    clippy::useless_conversion,
                    clippy::unnecessary_fallible_conversions,
                    clippy::identity_op,
                )]
                impl ::ipa_step::CompactStep for AllArms {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = <::some::other::StepEnum as ::ipa_step::CompactStep>::STEP_COUNT + 6;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex {
                        match self {
                            Self::Empty => 0,
                            Self::Int(i) => ::ipa_step::CompactGateIndex::try_from(*i).unwrap() + 1,
                            Self::Child => 4,
                            Self::Final => <::some::other::StepEnum as ::ipa_step::CompactStep>::STEP_COUNT + 5,
                        }
                    }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i == 0 => Self::Empty.as_ref().to_owned(),
                            _ if i < 4 => Self::Int(usize::try_from(i - (1)).unwrap()).as_ref().to_owned(),
                            _ if i == 4 => Self::Child.as_ref().to_owned(),
                            _ if i < <::some::other::StepEnum as ::ipa_step::CompactStep>::STEP_COUNT + 5
                                => Self::Child.as_ref().to_owned() + "/" + &<::some::other::StepEnum as ::ipa_step::CompactStep>::step_string(i - (5)),
                            _ if i == <::some::other::StepEnum as ::ipa_step::CompactStep>::STEP_COUNT + 5
                                => Self::Final.as_ref().to_owned(),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }

                    fn step_narrow_type(i: ::ipa_step::CompactGateIndex) -> Option<&'static str> {
                        match i {
                            _ if i == 4 => Some(::std::any::type_name::<::some::other::StepEnum>()),
                            _ if (5..<::some::other::StepEnum as ::ipa_step::CompactStep>::STEP_COUNT + 5).contains(&i)
                              => <::some::other::StepEnum as ::ipa_step::CompactStep>::step_narrow_type(i - (5)),
                            _ => None,
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn struct_empty() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                struct StructEmpty;
            },
            &quote! {
                impl ::ipa_step::Step for StructEmpty {}

                impl ::std::convert::AsRef<str> for StructEmpty {
                    fn as_ref(&self) -> &str {
                        "struct_empty"
                    }
                }

                impl ::ipa_step::CompactStep for StructEmpty {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = 1;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex { 0 }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i == 0 => String::from("struct_empty"),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn struct_child() {
        derive_success(
            quote! {
                #[derive(CompactStep)]
                #[step(child = Child)]
                struct StructEmpty;
            },
            &quote! {
                impl ::ipa_step::Step for StructEmpty {}

                impl ::std::convert::AsRef<str> for StructEmpty {
                    fn as_ref(&self) -> &str {
                        "struct_empty"
                    }
                }

                impl ::ipa_step::CompactStep for StructEmpty {
                    const STEP_COUNT: ::ipa_step::CompactGateIndex = <Child as ::ipa_step::CompactStep>::STEP_COUNT + 1;
                    fn base_index(&self) -> ::ipa_step::CompactGateIndex { 0 }
                    fn step_string(i: ::ipa_step::CompactGateIndex) -> String {
                        match i {
                            _ if i == 0 => String::from("struct_empty"),
                            _ if i < <Child as ::ipa_step::CompactStep>::STEP_COUNT + 1
                              => String::from ("struct_empty") + "/" + &<Child as ::ipa_step::CompactStep>::step_string(i - (1)),
                            _ => panic!("step {i} is not valid for {t}", t = ::std::any::type_name::<Self>()),
                        }
                    }
                    fn step_narrow_type(i: ::ipa_step::CompactGateIndex) -> Option<&'static str> {
                        match i {
                            _ if i == 0 => Some(::std::any::type_name::<Child>()),
                            _ if (1..<Child as ::ipa_step::CompactStep>::STEP_COUNT + 1).contains(&i)
                              => <Child as ::ipa_step::CompactStep>::step_narrow_type(i - (1)),
                            _ => None,
                        }
                    }
                }
            },
        );
    }

    #[test]
    fn struct_missing_count() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                struct Foo(u8);
            },
            "#[derive(CompactStep)] requires that integer variants include #[step(count = ...)]",
        );
    }

    #[test]
    fn union_unsupported() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                union Foo {};
            },
            "unexpected token",
        );
    }

    #[test]
    fn named_variant() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    Named {
                        n: u8,
                    }
                }
            },
            "#[derive(CompactStep)] does not support named field",
        );
    }

    #[test]
    fn with_discriminant() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    Bar = 1,
                }
            },
            "#[derive(CompactStep)] does not work with discriminants",
        );
    }

    #[test]
    fn empty_variant() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    Bar(),
                }
            },
            "#[derive(CompactStep) only supports empty or integer variants",
        );
    }

    #[test]
    fn tuple_variant() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    Bar((), u8),
                }
            },
            "#[derive(CompactStep) only supports empty or integer variants",
        );
    }

    #[test]
    fn empty_tuple_variant() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    Bar(()),
                }
            },
            "#[derive(CompactStep)] variants need to have a single integer type",
        );
    }

    #[test]
    fn count_unit() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(count = 10)]
                    Bar,
                }
            },
            "#[step(count = ...)] only applies to integer variants",
        );
    }

    #[test]
    fn count_str() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(count = "10")]
                    Bar(u8),
                }
            },
            "expected integer literal",
        );
    }

    #[test]
    fn count_too_small() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(count = 1)]
                    Bar(u8),
                }
            },
            "#[step(count = ...)] needs to be at least 2 and less than 1000",
        );
    }

    #[test]
    fn count_too_large() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(count = 10_000)]
                    Bar(u8),
                }
            },
            "#[step(count = ...)] needs to be at least 2 and less than 1000",
        );
    }

    #[test]
    fn two_count() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(count = 3, count = 3)]
                    Bar(u8),
                }
            },
            "#[step(count = ...)] duplicated",
        );
    }

    #[test]
    fn two_kids() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(child = Foo, child = Foo)]
                    Bar(u8),
                }
            },
            "#[step(child = ...)] duplicated",
        );
    }

    #[test]
    fn lit_kid() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(child = 3)]
                    Bar(u8),
                }
            },
            "expected identifier",
        );
    }

    #[test]
    fn two_names() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(name = "one", name = "two")]
                    Bar(u8),
                }
            },
            "#[step(name = ...)] duplicated",
        );
    }

    #[test]
    fn name_invalid() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(name = 12)]
                    Bar(u8),
                }
            },
            "expected string literal",
        );
    }

    #[test]
    fn name_slask() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(name = "/")]
                    Bar(u8),
                }
            },
            "#[step(name = ...)] cannot contain '/'",
        );
    }

    #[test]
    fn unsupported_argument() {
        derive_failure(
            quote! {
                #[derive(CompactStep)]
                enum Foo {
                    #[step(baz = 12)]
                    Bar(u8),
                }
            },
            "#[step(...)] unsupported argument",
        );
    }
}
