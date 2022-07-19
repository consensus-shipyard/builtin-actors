use crate::tcid::{TCid, THamt};
use cid::multihash::Code::Blake2b256;
use cid::multihash::MultihashDigest;
use cid::Cid;
use fvm_ipld_encoding::{serde_bytes, tuple::*, Cbor, RawBytes, DAG_CBOR};
use fvm_shared::MethodNum;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

pub const METHOD_LOCK: MethodNum = 2;
pub const METHOD_MERGE: MethodNum = 3;
pub const METHOD_ABORT: MethodNum = 4;
pub const METHOD_UNLOCK: MethodNum = 5;

#[derive(PartialEq, Eq, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct SerializedState {
    #[serde(with = "serde_bytes")]
    ser: Vec<u8>,
}
impl SerializedState {
    pub fn cid(&self) -> Cid {
        Cid::new_v1(DAG_CBOR, Blake2b256.digest(self.ser.as_slice()))
    }
}

#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct LockParams {
    pub method: MethodNum,
    pub params: RawBytes,
}
impl Cbor for LockParams {}
impl LockParams {
    pub fn new(method: MethodNum, params: RawBytes) -> Self {
        LockParams { method, params }
    }
}

#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct MergeParams<T: Serialize + DeserializeOwned> {
    state: T,
}
impl<T: Serialize + DeserializeOwned> Cbor for MergeParams<T> {}

#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct UnlockParams {
    params: LockParams,
    state: SerializedState, // FIXME: This is a locked state for the output. We may be able to use generics here.
}
impl Cbor for UnlockParams {}
impl UnlockParams {
    pub fn new(params: LockParams, state: SerializedState) -> Self {
        UnlockParams { params, state }
    }
}

#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct LockedState<T: Serialize + DeserializeOwned> {
    lock: bool,
    state: T,
}
impl<T: Serialize + DeserializeOwned> Cbor for LockedState<T> {}

pub trait LockableState<S: Serialize + DeserializeOwned> {
    fn merge(other: Self) -> anyhow::Result<()>;
    fn merge_output(other: Self) -> anyhow::Result<()>;
}

pub trait LockableActorState<T: Serialize + DeserializeOwned> {
    fn locked_map_cid() -> TCid<THamt<Cid, LockedState<T>>>;
    fn output(params: LockParams) -> LockedState<T>;
}

pub trait LockableActor<S: Serialize + DeserializeOwned + LockableActorState<S>> {
    fn lock(params: LockParams) -> anyhow::Result<Option<RawBytes>>;
    fn merge(params: MergeParams<S>) -> anyhow::Result<Option<RawBytes>>;
    fn unlock(params: UnlockParams) -> anyhow::Result<Option<RawBytes>>;
    fn abort(params: LockParams) -> anyhow::Result<Option<RawBytes>>;
    fn state(params: LockParams) -> S;
}

#[cfg(test)]
mod test {
    #[test]
    fn test_e2e_lock() {}
}
