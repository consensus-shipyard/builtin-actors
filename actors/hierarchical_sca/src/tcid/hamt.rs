use std::any::type_name;
use std::marker::PhantomData;

use crate::tcid_serde;
use anyhow::{anyhow, Result};
use cid::{multihash::Code, Cid};
use fil_actors_runtime::{make_empty_map, make_map_with_root_and_bitwidth};
use fvm_ipld_blockstore::{Blockstore, MemoryBlockstore};
use fvm_ipld_hamt::Hamt;
use fvm_shared::HAMT_BIT_WIDTH;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use super::{Content, Stored};

/// Static typing information for HAMT fields, a.k.a. `Map`.
///
/// # Example
/// ```
/// use fil_actor_hierarchical_sca::tcid::{CHamt, Stored};
/// use fvm_ipld_blockstore::MemoryBlockstore;
/// use fvm_ipld_encoding::tuple::*;
/// use fvm_ipld_encoding::Cbor;
/// use fvm_ipld_hamt::BytesKey;
///
/// #[derive(Serialize_tuple, Deserialize_tuple)]
/// struct MyType {
///   my_field: CHamt<String, u64>
/// }
/// impl Cbor for MyType {}
///
/// let store = MemoryBlockstore::new();
///
/// let mut my_inst = MyType {
///   my_field: CHamt::new(&store).unwrap()
/// };
///
/// let key = BytesKey::from("foo");
///
/// my_inst.my_field.update(&store, |x| {
///   x.set(key.clone(), 1).map_err(|e| e.into())
/// }).unwrap();
///
/// assert_eq!(&1, my_inst.my_field.load(&store).unwrap().get(&key).unwrap().unwrap())
/// ```
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct CHamt<K, V, const W: u32 = HAMT_BIT_WIDTH> {
    cid: Cid,
    _phantom_k: PhantomData<K>,
    _phantom_v: PhantomData<V>,
}

impl<K, V, const W: u32> From<Cid> for CHamt<K, V, W> {
    fn from(cid: Cid) -> Self {
        CHamt { cid, _phantom_k: PhantomData, _phantom_v: PhantomData }
    }
}

impl<K, V, const W: u32> Content for CHamt<K, V, W> {
    fn cid(&self) -> Cid {
        self.cid
    }

    fn code(&self) -> Code {
        Code::Blake2b256
    }
}

tcid_serde!(CHamt<K, V, W const: u32>);

impl<K, V, const W: u32> CHamt<K, V, W>
where
    V: Serialize + DeserializeOwned,
{
    /// Initialize an empty data structure, flush it to the store and capture the `Cid`.
    pub fn new<S: Blockstore>(store: &S) -> Result<Self> {
        let cid = make_empty_map::<_, V>(store, W)
            .flush()
            .map_err(|e| anyhow!("Failed to create empty map: {}", e))?;

        Ok(Self::from(cid))
    }
}

impl<'s, S: 's + Blockstore, K, V, const W: u32> Stored<'s, S> for CHamt<K, V, W>
where
    V: Serialize + DeserializeOwned,
{
    type Item = Hamt<&'s S, V>;

    fn load(&self, store: &'s S) -> Result<Self::Item> {
        make_map_with_root_and_bitwidth::<S, V>(&self.cid, store, W)
            .map_err(|e| anyhow!("error loading {}: {}", type_name::<Self>(), e))
    }

    fn flush(&mut self, mut value: Self::Item) -> Result<Self::Item> {
        let cid =
            value.flush().map_err(|e| anyhow!("error flushing {}: {}", type_name::<Self>(), e))?;
        self.cid = cid;
        Ok(value)
    }
}

/// This `Default` implementation is unsound in that while it
/// creates `CHamt` instances with a correct `Cid` value, this value
/// is not stored anywhere, so there is no guarantee that any retrieval
/// attempt from a random store won't fail.
///
/// The main purpose is to allow the `#[derive(Default)]` to be
/// applied on types that use a `CHamt` field, if that's unavoidable.
impl<K, V, const W: u32> Default for CHamt<K, V, W>
where
    V: Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self::new(&MemoryBlockstore::new()).unwrap()
    }
}
