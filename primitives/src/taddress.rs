use std::{convert::TryFrom, fmt::Display, marker::PhantomData, str::FromStr};

use serde::de::Error;

use fvm_ipld_encoding::Cbor;
use fvm_shared::address::{Address, Payload, SubnetID};

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TAddress<T> {
    addr: Address,
    _phantom: PhantomData<T>,
}

impl<T> TAddress<T> {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.addr.to_bytes()
    }

    /// The untyped `Address` representation.
    pub fn addr(&self) -> &Address {
        &self.addr
    }
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

impl<A: RawAddress> TryFrom<Address> for TAddress<A> {
    type Error = fvm_shared::address::Error;

    fn try_from(value: Address) -> Result<Self, Self::Error> {
        if !A::is_compatible(value) {
            return Err(fvm_shared::address::Error::InvalidPayload);
        }
        Ok(Self { addr: value, _phantom: PhantomData })
    }
}

impl<A> TAddress<Hierarchical<A>> {
    pub fn subnet(&self) -> SubnetID {
        self.addr.subnet().unwrap()
    }

    pub fn raw_addr(&self) -> TAddress<A> {
        TAddress { addr: self.addr.raw_addr().unwrap(), _phantom: PhantomData }
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

/// Apparently CBOR has problems using `Address` as a key in `HashMap`.
/// This type can be used to wrap an address and turn it into `String`
/// for the purpose of CBOR serialization.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct TAddressKey<T>(pub TAddress<T>);

/// Serializes to the `String` format of the underlying `Address`.
impl<T> serde::Serialize for TAddressKey<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.addr.to_string().serialize(serializer)
    }
}

/// Deserializes from `String` format. May be rejected if the address is not the expected type.
impl<'d, T> serde::Deserialize<'d> for TAddressKey<T>
where
    TAddress<T>: TryFrom<Address>,
    <TAddress<T> as TryFrom<Address>>::Error: Display,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'d>,
    {
        let str = String::deserialize(deserializer)?;
        let raw = Address::from_str(&str)
            .map_err(|e| D::Error::custom(format!("not an address string: {}", e)))?;
        let addr = TAddress::<T>::try_from(raw)
            .map_err(|e| D::Error::custom(format!("wrong address type: {}", e)))?;
        Ok(Self(addr))
    }
}

impl<T> Cbor for TAddressKey<T>
where
    TAddress<T>: TryFrom<Address>,
    <TAddress<T> as TryFrom<Address>>::Error: Display,
{
}
