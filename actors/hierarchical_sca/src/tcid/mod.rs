use cid::{multihash::Code, Cid};

mod amt;
mod cref;
mod hamt;
pub use amt::CAmt;
pub use cref::CRef;
pub use hamt::CHamt;

/// Helper type to be able to define `Code` as a generic parameter.
pub trait CodeType {
    fn code() -> Code;
}

/// `TCid` is typed content, represented by a `Cid`.
pub trait TCid: From<Cid> {
    fn cid(&self) -> Cid;
    fn code(&self) -> Code;
}

/// Assuming that the type implements `load` and `flush`, implement some convenience methods.
///
/// NOTE: This can be achieved with a trait and an associated type as well, but unfortunately
/// it got too complex for Rust Analyzer to provide code completion, which makes it less ergonomic.
/// At least this way there's no need to import the trait that contains these ops.
#[macro_export]
macro_rules! tcid_ops {
    (
        $typ:ident < $($gen:ident $($const:ident)? $(: $b:ident $(+ $bs:ident)* )? ),+ > => $item:ty
    ) => {
        /// Operations on content types that, once loaded, are rooted
        /// and bound to a block store, and need to be flushed back.
        impl< $($($const)? $gen $(: $b $(+ $bs)* )? ),+ > $typ<$($gen),+>
        {
            /// Load, modify and flush a value, returning something as a result.
            pub fn modify<'s, S: fvm_ipld_blockstore::Blockstore, R>(
                &mut self,
                store: &'s S,
                f: impl FnOnce(&mut $item) -> anyhow::Result<R>,
            ) -> anyhow::Result<R> {
                let mut value = self.load(store)?;
                let result = f(&mut value)?;
                self.flush(value)?;
                Ok(result)
            }

            /// Load, modify and flush a value.
            pub fn update<'s, S: fvm_ipld_blockstore::Blockstore, R>(
                &mut self,
                store: &'s S,
                f: impl FnOnce(&mut $item) -> anyhow::Result<R>,
            ) -> anyhow::Result<()> {
                self.modify(store, |x| f(x)).map(|_| ())
            }
        }
    }
}

/// Define serde types for anything that implements `TCid`.
///
/// NOTE: For technical reasons the `const` keyword comes
/// *after* the generic variable that it applies to.
#[macro_export]
macro_rules! tcid_serde {
    (
        $typ:ident < $($gen:ident $($const:ident)? $(: $b:ident $(+ $bs:ident)* )? ),+ >
    ) => {
        /// `Content` serializes exactly as its underlying `Cid`.
        impl < $($($const)? $gen $(: $b $(+ $bs)* )? ),+ > serde::Serialize for $typ<$($gen),+>
        {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                self.cid.serialize(serializer)
            }
        }

        /// `Content` deserializes exactly as its underlying `Cid`.
        impl<'d, $($($const)? $gen $(: $b $(+ $bs)* )? ),+ > serde::Deserialize<'d> for $typ<$($gen),+>
        where Self: From<Cid>
        {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'d>,
            {
                let cid = Cid::deserialize(deserializer)?;
                Ok(Self::from(cid))
            }
        }
    }
}

pub mod codes {
    use super::CodeType;

    /// Define a unit struct for a `Code` element that
    /// can be used as a generic parameter.
    macro_rules! code_types {
        ($($code:ident => $typ:ident),+) => {
            $(
            #[derive(PartialEq, Eq, Clone, Debug)]
            pub struct $typ;

            impl CodeType for $typ {
                fn code() -> cid::multihash::Code {
                    cid::multihash::Code::$code
                }
            }
            )*
        };
    }

    // XXX: For some reason none of the other code types work,
    // not even on their own as a variable:
    // let c = multihash::Code::Keccak256;
    // ERROR: no variant or associated item named `Keccak256` found for enum `Code`
    //        in the current scope variant or associated item not found in `Code`
    code_types! {
      Blake2b256 => Blake2b256
    }
}

#[cfg(test)]
mod test {
    use super::{CHamt, CRef, TCid};
    use cid::Cid;
    use fvm_ipld_blockstore::MemoryBlockstore;
    use fvm_ipld_encoding::tuple::*;
    use fvm_ipld_hamt::BytesKey;

    #[derive(Default, Serialize_tuple, Deserialize_tuple, PartialEq)]
    struct TestRecord {
        foo: u64,
        bar: Vec<u8>,
    }

    #[derive(Default, Serialize_tuple, Deserialize_tuple)]
    struct TestRecordTyped {
        pub optional: Option<CRef<TestRecord>>,
        pub map: CHamt<String, TestRecord>,
    }

    impl TestRecordTyped {
        fn new(store: &MemoryBlockstore) -> Self {
            Self { optional: None, map: CHamt::new(store).unwrap() }
        }
    }

    #[derive(Default, Serialize_tuple, Deserialize_tuple)]
    struct TestRecordUntyped {
        pub optional: Option<Cid>,
        pub map: Cid,
    }

    #[test]
    fn default_cid_and_default_hamt_differ() {
        let cid_typed: CRef<TestRecordTyped> = CRef::default();
        let cid_untyped: CRef<TestRecordUntyped> = CRef::default();
        // The stronger typing allows us to use proper default values,
        // but this is a breaking change from the invalid values that came before.
        assert_ne!(cid_typed.cid(), cid_untyped.cid());
    }

    #[test]
    fn default_value_read_fails() {
        let cid_typed: CRef<TestRecordTyped> = CRef::default();
        let store = MemoryBlockstore::new();
        assert!(cid_typed.load(&store).is_err());
    }

    // #[test]
    // fn ref_modify() {
    //     let store = MemoryBlockstore::new();
    //     let mut r: CRef<TestRecord> = CRef::new(&store, &TestRecord::default()).unwrap();

    //     let mut c = r.load(&store).unwrap();
    // }

    #[test]
    fn hamt_modify() {
        let store = MemoryBlockstore::new();
        let mut rec = TestRecordTyped::new(&store);

        let eggs = rec
            .map
            .modify(&store, |map| {
                map.set(BytesKey::from("spam"), TestRecord { foo: 1, bar: Vec::new() })?;
                Ok("eggs")
            })
            .unwrap();
        assert_eq!(eggs, "eggs");

        let map = rec.map.load(&store).unwrap();

        let foo = map.get(&BytesKey::from("spam")).unwrap().map(|x| x.foo);
        assert_eq!(foo, Some(1))
    }
}
