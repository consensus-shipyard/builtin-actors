use std::any::type_name;
use std::marker::PhantomData;

use super::{codes, CodeType, Content, Stored};
use crate::tcid_serde;
use anyhow::{anyhow, Result};
use cid::{multihash, Cid};
use fvm_ipld_blockstore::{Blockstore, MemoryBlockstore};
use fvm_ipld_encoding::CborStore;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::ops::{Deref, DerefMut};

/// Static typing information for `Cid` fields to help read and write data safely.
///
/// # Example
/// ```
/// use fil_actor_hierarchical_sca::tcid::{CRef, Stored};
/// use fvm_ipld_blockstore::MemoryBlockstore;
/// use fvm_ipld_encoding::tuple::*;
/// use fvm_ipld_encoding::Cbor;
///
/// #[derive(Serialize_tuple, Deserialize_tuple)]
/// struct MyType {
///   my_field: u64
/// }
/// impl Cbor for MyType {}
///
/// let store = MemoryBlockstore::new();
///
/// let mut my_ref: CRef<MyType> = CRef::new(&store, &MyType { my_field: 0 }).unwrap();
///
/// my_ref.update(&store, |x| {
///   x.my_field += 1;
///   Ok(())
/// }).unwrap();
///
/// assert_eq!(1, my_ref.load(&store).unwrap().my_field);
/// ```
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct CRef<T, C = codes::Blake2b256> {
    cid: Cid,
    _phantom_t: PhantomData<T>,
    _phantom_c: PhantomData<C>,
}

impl<T, C: CodeType> From<Cid> for CRef<T, C> {
    fn from(cid: Cid) -> Self {
        CRef { cid, _phantom_t: PhantomData, _phantom_c: PhantomData }
    }
}

impl<T, C: CodeType> Content for CRef<T, C> {
    fn cid(&self) -> Cid {
        self.cid
    }

    fn code(&self) -> multihash::Code {
        C::code()
    }
}

tcid_serde!(CRef<T, C>);

pub struct StoreContent<'s, S: Blockstore, T> {
    store: &'s S,
    content: T,
}

impl<'s, S: 's + Blockstore, T> Deref for StoreContent<'s, S, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.content
    }
}

impl<'s, S: 's + Blockstore, T> DerefMut for StoreContent<'s, S, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.content
    }
}

/// Operations on primitive types that can directly be read/written from/to CBOR.
impl<T, C: CodeType> CRef<T, C>
where
    T: Serialize + DeserializeOwned,
{
    /// Initialize a `CRef` by storing a value as CBOR in the store and capturing the `Cid`.
    pub fn new<S: Blockstore>(store: &S, value: &T) -> Result<Self> {
        let cid = store.put_cbor(value, C::code())?;
        Ok(Self::from(cid))
    }
}

impl<'s, S: 's + Blockstore, T, C: CodeType> Stored<'s, S> for CRef<T, C>
where
    T: Serialize + DeserializeOwned,
{
    type Item = StoreContent<'s, S, T>;

    /// Read the underlying `Cid` from the store or return an error if not found.
    fn load(&self, store: &'s S) -> Result<Self::Item> {
        match store.get_cbor(&self.cid)? {
            Some(content) => Ok(StoreContent { store, content }),
            None => Err(anyhow!(
                "error loading {}: Cid ({}) did not match any in database",
                type_name::<Self>(),
                self.cid.to_string()
            )),
        }
    }

    /// Put the value into the store and overwrite the `Cid`.
    fn flush(&mut self, value: Self::Item) -> Result<Self::Item> {
        let cid = value.store.put_cbor(&value.content, C::code())?;
        self.cid = cid;
        Ok(value)
    }
}

/// This `Default` implementation is unsound in that while it
/// creates `CRef` instances with a correct `Cid` value, this value
/// is not stored anywhere, so there is no guarantee that any retrieval
/// attempt from a random store won't fail.
///
/// The main purpose is to allow the `#[derive(Default)]` to be
/// applied on types that use a `CRef` field, if that's unavoidable.
impl<T, C: CodeType> Default for CRef<T, C>
where
    T: Serialize + DeserializeOwned + Default,
{
    fn default() -> Self {
        Self::new(&MemoryBlockstore::new(), &T::default()).unwrap()
    }
}
