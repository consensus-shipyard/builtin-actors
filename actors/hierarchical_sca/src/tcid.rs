use std::marker::PhantomData;

use anyhow::{anyhow, Error, Result};
use cid::{multihash, Cid};
use fil_actors_runtime::{make_empty_map, make_map_with_root_and_bitwidth};
use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_encoding::{tuple::*, Cbor, CborStore};
use fvm_ipld_hamt::Hamt;

#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct TCid<T> {
    cid: Cid,
    name: String,
    code: multihash::Code,
    _phantom: PhantomData<T>,
}

pub struct THamt<K, V, const W: u32> {
    _phantom_k: PhantomData<K>,
    _phantom_v: PhantomData<V>,
}

impl<T: Cbor> TCid<T> {
    pub fn get_cbor<S: Blockstore>(&self, store: &S) -> Result<Option<T>> {
        store.get_cbor(&self.cid)
    }

    pub fn put_cbor<S: Blockstore>(&mut self, store: &S, value: &T) -> Result<()> {
        let cid = store.put_cbor(value, self.code)?;
        self.cid = cid;
        Ok(())
    }
}

impl<K, V: Cbor, const W: u32> TCid<THamt<K, V, W>> {
    pub fn get_cbor<'s, S: Blockstore>(&self, store: &'s S) -> Result<Hamt<&'s S, V>, Error> {
        make_map_with_root_and_bitwidth::<S, V>(&self.cid, store, W)
            .map_err(|e| anyhow!("error loading {}: {}", self.name, e))
    }

    pub fn flush_cbor<'s, S: Blockstore>(&mut self, value: &mut Hamt<&'s S, V>) -> Result<()> {
        let cid = value.flush().map_err(|e| anyhow!("error flushing {}: {}", self.name, e))?;
        self.cid = cid;
        Ok(())
    }

    pub fn new_hamt<S: Blockstore>(store: &S, name: String, code: multihash::Code) -> Result<Self> {
        let cid = make_empty_map::<_, V>(store, W)
            .flush()
            .map_err(|e| anyhow!("Failed to create empty map: {}", e))?;

        Ok(TCid { cid, name, code, _phantom: PhantomData })
    }
}

#[cfg(test)]
mod test {
    use super::{TCid, THamt};
    use crate::Checkpoint;
    use anyhow::Result;
    use cid::{multihash, Cid};
    use fil_actors_runtime::ActorDowncast;
    use fvm_ipld_blockstore::Blockstore;
    use fvm_ipld_encoding::{tuple::*, Cbor};
    use fvm_ipld_hamt::BytesKey;
    use fvm_shared::{clock::ChainEpoch, HAMT_BIT_WIDTH};

    #[derive(Serialize_tuple, Deserialize_tuple)]
    struct State {
        pub child_state: Option<TCid<State>>,
        pub checkpoints: TCid<THamt<ChainEpoch, Checkpoint, HAMT_BIT_WIDTH>>,
    }

    impl Cbor for State {}

    impl State {
        pub fn new<S: Blockstore>(store: &S) -> Result<Self> {
            Ok(Self {
                child_state: None,
                checkpoints: TCid::new_hamt(
                    store,
                    "checkpoints".to_owned(),
                    multihash::Code::Blake2b256,
                )?,
            })
        }

        /// flush a checkpoint
        pub(crate) fn flush_checkpoint<BS: Blockstore>(
            &mut self,
            store: &BS,
            ch: &Checkpoint,
        ) -> anyhow::Result<()> {
            let mut checkpoints = self.checkpoints.get_cbor(store)?;

            let epoch = ch.epoch();
            checkpoints.set(BytesKey::from(epoch.to_ne_bytes().to_vec()), ch.clone()).map_err(
                |e| e.downcast_wrap(format!("failed to set checkpoint for epoch {}", epoch)),
            )?;

            self.checkpoints.flush_cbor(&mut checkpoints)
        }
    }
}
