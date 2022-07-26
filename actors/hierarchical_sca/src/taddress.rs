use std::{convert::TryFrom, fmt::Display, marker::PhantomData};

use serde::de::Error;

use fvm_ipld_encoding::Cbor;
use fvm_shared::address::{Address, SubnetID};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TAddress<T> {
    addr: Address,
    _phantom: PhantomData<T>,
}

/// Define a unit struct for address types that can be used as a generic parameter.
macro_rules! address_types {
    ($($typ:ident),+) => {
        $(
        #[derive(PartialEq, Eq, Hash, Clone, Debug)]
        pub struct $typ;
        )*
    };
}

// Based on `Payload` variants.
address_types! {
  ID,
  Secp256k1,
  Actor,
  BLS,
  Hierarchical
}

// NOTE: `Hierarchical` could also be generic in what it wraps,
// which could be `Any`, `ID, ``Secp256k1`, `BLS` but *not* another `Hierarchical`.

impl<T> Into<Address> for TAddress<T> {
    fn into(self) -> Address {
        self.addr
    }
}

impl TryFrom<Address> for TAddress<Hierarchical> {
    type Error = fvm_shared::address::Error;

    fn try_from(value: Address) -> Result<Self, Self::Error> {
        let sub = value.subnet()?;
        let raw = value.raw_addr()?;
        let addr = Address::new_hierarchical(&sub, &raw)?;
        Ok(Self { addr, _phantom: PhantomData })
    }
}

impl TAddress<Hierarchical> {
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
