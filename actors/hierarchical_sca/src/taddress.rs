use std::{convert::TryFrom, fmt::Display, marker::PhantomData};

use serde::de::Error;

use fvm_ipld_encoding::Cbor;
use fvm_shared::address::{Address, Payload, SubnetID};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TAddress<T> {
    addr: Address,
    _phantom: PhantomData<T>,
}

trait RawAddress {
    fn is_compatible(addr: Address) -> bool;
}

/// Define a unit struct for address types that can be used as a generic parameter.
macro_rules! raw_address_types {
    ($($typ:ident),+) => {
        $(
        #[derive(PartialEq, Eq, Hash, Clone, Debug)]
        pub struct $typ;

        impl RawAddress for $typ {
          fn is_compatible(addr: Address) -> bool {
            match addr.payload() {
              Payload::$typ(_) => true,
              _ => false
            }
          }
        }
        )*
    };
}

// Based on `Payload` variants.
raw_address_types! {
  ID,
  Secp256k1,
  Actor,
  BLS
}

/// For `Hierarchical` address type that doesn't say what kind it wraps.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct AnyRawAddr;

impl RawAddress for AnyRawAddr {
    fn is_compatible(addr: Address) -> bool {
        match addr.payload() {
            Payload::Hierarchical(_) => false,
            _ => true,
        }
    }
}

/// A `Hierarchical` is generic in what it wraps, which could be any raw address type, but *not* another `Hierarchical`.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct Hierarchical<A> {
    _phantom: PhantomData<A>,
}

impl<T> Into<Address> for TAddress<T> {
    fn into(self) -> Address {
        self.addr
    }
}

impl<A: RawAddress> TryFrom<Address> for TAddress<Hierarchical<A>> {
    type Error = fvm_shared::address::Error;

    fn try_from(value: Address) -> Result<Self, Self::Error> {
        let sub = value.subnet()?;
        let raw = value.raw_addr()?;
        if !A::is_compatible(raw) {
            return Err(fvm_shared::address::Error::InvalidPayload);
        }
        let addr = Address::new_hierarchical(&sub, &raw)?;
        Ok(Self { addr, _phantom: PhantomData })
    }
}

impl<A> TAddress<Hierarchical<A>> {
    pub fn subnet(&self) -> SubnetID {
        self.addr.subnet().unwrap()
    }

    pub fn raw_addr(&self) -> Address {
        self.addr.raw_addr().unwrap()
    }
}

/// Serializes exactly as its underlying `Address`.
impl<T> serde::Serialize for TAddress<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.addr.serialize(serializer)
    }
}

/// Deserializes exactly as its underlying `Address` but might be rejected if it's not the expected type.
impl<'d, T> serde::Deserialize<'d> for TAddress<T>
where
    Self: TryFrom<Address>,
    <Self as TryFrom<Address>>::Error: Display,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'d>,
    {
        let raw = Address::deserialize(deserializer)?;
        match Self::try_from(raw) {
            Ok(addr) => Ok(addr),
            Err(e) => Err(D::Error::custom(format!("wrong address type: {}", e))),
        }
    }
}

impl<T> Cbor for TAddress<T>
where
    Self: TryFrom<Address>,
    <Self as TryFrom<Address>>::Error: Display,
{
}

impl<T> TAddress<T> {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.addr.to_bytes()
    }
}
