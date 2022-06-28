use std::any::type_name;
use std::marker::PhantomData;

use crate::tcid_serde;

use super::{Content, Stored};
use anyhow::{anyhow, Result};
use cid::multihash::Code;
use cid::Cid;
use fil_actors_runtime::fvm_ipld_amt::Amt;
use fvm_ipld_blockstore::{Blockstore, MemoryBlockstore};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

/// Same as `fvm_ipld_amt::DEFAULT_BIT_WIDTH`.
const AMT_BIT_WIDTH: u32 = 3;

/// Static typing information for AMT fields, a.k.a. `Array`.
///
/// # Example
/// ```
/// use fil_actor_hierarchical_sca::tcid::{CAmt, Stored};
/// use fvm_ipld_blockstore::MemoryBlockstore;
/// use fvm_ipld_encoding::tuple::*;
/// use fvm_ipld_encoding::Cbor;
///
/// #[derive(Serialize_tuple, Deserialize_tuple)]
/// struct MyType {
///   my_field: CAmt<String>
/// }
/// impl Cbor for MyType {}
///
/// let store = MemoryBlockstore::new();
///
/// let mut my_inst = MyType {
///   my_field: CAmt::new(&store).unwrap()
/// };
///
/// my_inst.my_field.update(&store, |x| {
///   x.set(0, "bar".into()).map_err(|e| e.into())
/// }).unwrap();
///
/// assert_eq!(&"bar", my_inst.my_field.load(&store).unwrap().get(0).unwrap().unwrap())
/// ```
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct CAmt<V, const W: u32 = AMT_BIT_WIDTH> {
    cid: Cid,
    _phantom_v: PhantomData<V>,
}

impl<V, const W: u32> From<Cid> for CAmt<V, W> {
    fn from(cid: Cid) -> Self {
        CAmt { cid, _phantom_v: PhantomData }
    }
}

impl<V, const W: u32> Content for CAmt<V, W> {
    fn cid(&self) -> Cid {
        self.cid
    }

    fn code(&self) -> Code {
        Code::Blake2b256
    }
}

tcid_serde!(CAmt<V, W const: u32>);

impl<V, const W: u32> CAmt<V, W>
where
    V: Serialize + DeserializeOwned,
{
    /// Initialize an empty data structure, flush it to the store and capture the `Cid`.
    pub fn new<S: Blockstore>(store: &S) -> Result<Self> {
        let cid = Amt::<V, _>::new_with_bit_width(store, W)
            .flush()
            .map_err(|e| anyhow!("Failed to create empty array: {}", e))?;

        Ok(Self::from(cid))
    }
}

impl<'s, S: 's + Blockstore, V, const W: u32> Stored<'s, S> for CAmt<V, W>
where
    V: Serialize + DeserializeOwned,
{
    type Item = Amt<V, &'s S>;

    fn load(&self, store: &'s S) -> Result<Self::Item> {
        Amt::<V, _>::load(&self.cid, store)
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
/// creates `CAmt` instances with a correct `Cid` value, this value
/// is not stored anywhere, so there is no guarantee that any retrieval
/// attempt from a random store won't fail.
///
/// The main purpose is to allow the `#[derive(Default)]` to be
/// applied on types that use a `CAmt` field, if that's unavoidable.
impl<V, const W: u32> Default for CAmt<V, W>
where
    V: Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self::new(&MemoryBlockstore::new()).unwrap()
    }
}
