use std::{any::type_name, marker::PhantomData};

use anyhow::{anyhow, Error, Result};
use cid::Cid;
use fil_actors_runtime::{
    builtin::HAMT_BIT_WIDTH, make_empty_map, make_map_with_root_and_bitwidth,
};
use fvm_ipld_blockstore::{Blockstore, MemoryBlockstore};
use fvm_ipld_encoding::{Cbor, CborStore};
use fvm_ipld_hamt::Hamt;

/// Helper type to be able to define `Code` as a generic parameter.
pub trait CodeType {
    fn code() -> cid::multihash::Code;
}

pub mod codes {
    use super::CodeType;

    /// Define a unit struct for a `Code` element that
    /// can be used as a generic parameter.
    macro_rules! code_types {
    ($($code:ident => $typ:ident),+) => {
        $(
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

/// Static typing information for `Cid` fields to help
/// read and write data safely.
pub struct TCid<T, C = codes::Blake2b256> {
    cid: Cid,
    _phantom_t: PhantomData<T>,
    _phantom_c: PhantomData<C>,
}

/// Static typing information for HAMT fields.
pub struct THamt<K, V, const W: u32 = HAMT_BIT_WIDTH> {
    _phantom_k: PhantomData<K>,
    _phantom_v: PhantomData<V>,
}

impl<T, C> serde::Serialize for TCid<T, C> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.cid.serialize(serializer)
    }
}

impl<'d, T, C> serde::Deserialize<'d> for TCid<T, C> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'d>,
    {
        let cid = Cid::deserialize(deserializer)?;
        Ok(TCid { cid, _phantom_t: PhantomData, _phantom_c: PhantomData })
    }
}

/// Operations on primitive types that can directly be read/written from/to CBOR.
impl<T: Cbor, C: CodeType> TCid<T, C> {
    pub fn new_cbor(value: &T) -> Result<Self> {
        let store = MemoryBlockstore::new();
        let cid = store.put_cbor(value, C::code())?;
        Ok(TCid { cid, _phantom_t: PhantomData, _phantom_c: PhantomData })
    }

    pub fn get_cbor<S: Blockstore>(&self, store: &S) -> Result<Option<T>> {
        store.get_cbor(&self.cid)
    }

    pub fn put_cbor<S: Blockstore>(&mut self, store: &S, value: &T) -> Result<()> {
        let cid = store.put_cbor(value, C::code())?;
        self.cid = cid;
        Ok(())
    }
}

/// Operations for HAMT types that, once read, hold a reference to the underlying storage.
impl<K, V: Cbor, const W: u32> TCid<THamt<K, V, W>, codes::Blake2b256> {
    pub fn new_hamt<S: Blockstore>(store: &S) -> Result<Self> {
        let cid = make_empty_map::<_, V>(store, W)
            .flush()
            .map_err(|e| anyhow!("Failed to create empty map: {}", e))?;

        Ok(TCid { cid, _phantom_t: PhantomData, _phantom_c: PhantomData })
    }

    pub fn get_hamt<'s, S: Blockstore>(&self, store: &'s S) -> Result<Hamt<&'s S, V>, Error> {
        make_map_with_root_and_bitwidth::<S, V>(&self.cid, store, W)
            .map_err(|e| anyhow!("error loading {}: {}", type_name::<Self>(), e))
    }

    pub fn flush_hamt<'s, S: Blockstore>(&mut self, value: &mut Hamt<&'s S, V>) -> Result<()> {
        let cid =
            value.flush().map_err(|e| anyhow!("error flushing {}: {}", type_name::<Self>(), e))?;
        self.cid = cid;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{TCid, THamt};
    use crate::Checkpoint;
    use anyhow::Result;
    use fil_actors_runtime::ActorDowncast;
    use fvm_ipld_blockstore::Blockstore;
    use fvm_ipld_encoding::{tuple::*, Cbor};
    use fvm_ipld_hamt::BytesKey;
    use fvm_shared::clock::ChainEpoch;

    #[derive(Serialize_tuple, Deserialize_tuple)]
    struct State {
        pub child_state: Option<TCid<State>>,
        pub checkpoints: TCid<THamt<ChainEpoch, Checkpoint>>,
    }

    impl Cbor for State {}

    impl State {
        pub fn new<S: Blockstore>(store: &S) -> Result<Self> {
            Ok(Self { child_state: None, checkpoints: TCid::new_hamt(store)? })
        }

        /// flush a checkpoint
        pub(crate) fn flush_checkpoint<BS: Blockstore>(
            &mut self,
            store: &BS,
            ch: &Checkpoint,
        ) -> anyhow::Result<()> {
            let mut checkpoints = self.checkpoints.get_hamt(store)?;

            let epoch = ch.epoch();
            checkpoints.set(BytesKey::from(epoch.to_ne_bytes().to_vec()), ch.clone()).map_err(
                |e| e.downcast_wrap(format!("failed to set checkpoint for epoch {}", epoch)),
            )?;

            self.checkpoints.flush_hamt(&mut checkpoints)
        }
    }
}
