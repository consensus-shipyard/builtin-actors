use cid::multihash::Code::Blake2b256;
use cid::multihash::MultihashDigest;
use cid::Cid;
use fil_actors_runtime::cbor;
use fvm_ipld_encoding::{serde_bytes, tuple::*, Cbor, RawBytes, DAG_CBOR};
use fvm_shared::MethodNum;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use crate::tcid::{TCid, THamt};

/// MethodNum to lock some state in an actor
/// This methods are only supported in actors
/// that support atomic executions.
pub const METHOD_LOCK: MethodNum = 2;
/// MethodNum used to trigger the merge of an input with
/// other input locked states.
pub const METHOD_MERGE: MethodNum = 3;
/// MethodNum called to signal the abortion of an atomic execution
/// and the unlock of all locked states in the actor for the execution
pub const METHOD_ABORT: MethodNum = 4;
/// MethodNum to trigger the merge of the output of an execution
/// into the state of an actor, and the unlock of all locked states.
pub const METHOD_UNLOCK: MethodNum = 5;

/// Trait that determines the functions that need to be implemented by
/// a state object to be lockable and be used in an atomic execution.
///
/// Different strategies may be used to merge different locked state to
/// prepare the actor state for the execution, and for the merging of the
/// output of the execution to the original state of the actor.
pub trait LockableState<S: Serialize + DeserializeOwned> {
    /// Merge a locked state (not necessarily the output) to the current state.
    fn merge(&mut self, other: Self) -> anyhow::Result<()>;
    /// Merge the output of an execution to the current state.
    fn merge_output(&mut self, other: Self) -> anyhow::Result<()>;
}

/// Trait that specifies the interface of an actor state able to support
/// atomic executions.
pub trait LockableActorState<T>
where
    T: Serialize + DeserializeOwned + LockableState<T>,
{
    /// Map with all the locked state in the actor uniquely identified through
    /// their Cid.
    fn locked_map_cid() -> TCid<THamt<Cid, LockedState<T>>>;
    /// Returns the output state of an execution from the current state
    /// of the actor according to the input parameters.
    fn output(params: LockParams) -> LockedState<T>;
}

/// Trait for an actor able to support an atomic execution.
pub trait LockableActor<T, S>
where
    T: Serialize + DeserializeOwned + LockableState<T>,
    S: Serialize + DeserializeOwned + LockableActorState<T>,
{
    /// Locks the state to perform the execution determined by the locking params.
    fn lock(params: LockParams) -> anyhow::Result<Option<RawBytes>>;
    /// Merges some state to the current state of the actor to prepare for the execution
    /// of the protocol.
    fn merge(params: MergeParams<T>) -> anyhow::Result<Option<RawBytes>>;
    /// Merges the output state of an execution to the actor and unlocks the state
    /// involved in the execution.
    fn unlock(params: UnlockParams) -> anyhow::Result<Option<RawBytes>>;
    /// Aborts the execution and unlocks the locked state.
    fn abort(params: LockParams) -> anyhow::Result<Option<RawBytes>>;
    /// Returns the lockable state of the actor.
    fn state(params: LockParams) -> S;
}

/// Serialized representation of the locked state of an actor.
#[derive(Debug, PartialEq, Eq, Clone, Serialize_tuple, Deserialize_tuple, Default)]
pub struct SerializedState {
    #[serde(with = "serde_bytes")]
    ser: Vec<u8>,
}
impl SerializedState {
    // TODO: This is used for testing purposes in order to have all the
    // SCA functions running. In the next iteration we will implement proper
    // primitives to get from/to a LockableState to SerializedState using
    // code-gen and generics.
    pub fn new(ser: Vec<u8>) -> Self {
        SerializedState { ser }
    }
    pub fn cid(&self) -> Cid {
        Cid::new_v1(DAG_CBOR, Blake2b256.digest(self.ser.as_slice()))
    }
}

/// Parameters used to lock certain state of an actor for its use in an atomic
/// execution
///
/// Different locking strategies may be implemented in the actor according to the
/// method and parameters used in the atomic execution. This parameters gives
/// information to the actor about the execution to be performed and thus the state
/// that needs to be locked.
#[derive(Debug, Eq, PartialEq, Serialize_tuple, Deserialize_tuple)]
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

/// Parameters used to specify the input state to merge to the current
/// state of an actor to perform the atomic execution.
#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct MergeParams<T>
where
    T: Serialize + DeserializeOwned + LockableState<T>,
{
    state: T,
}
impl<T: Serialize + DeserializeOwned + LockableState<T>> Cbor for MergeParams<T> {}

/// Unlock parameters that pass the output of the execution as the serialized
/// output state of the execution, along with the lock parameters that determines
/// the type of execution being performed and thus the merging strategy that needs
/// to be followed by the actor.
#[derive(Debug, Eq, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct UnlockParams {
    pub params: LockParams,
    pub state: SerializedState, // FIXME: This is a locked state for the output. We may be able to use generics here.
}
impl Cbor for UnlockParams {}
impl UnlockParams {
    pub fn new(params: LockParams, state: SerializedState) -> Self {
        UnlockParams { params, state }
    }
    pub fn from_raw_bytes(ser: &RawBytes) -> anyhow::Result<Self> {
        Ok(cbor::deserialize_params(ser)?)
    }
}

/// State of an actor including a lock to support atomic executions.
#[derive(Serialize_tuple, Deserialize_tuple)]
pub struct LockedState<T>
where
    T: Serialize + DeserializeOwned + LockableState<T>,
{
    lock: bool,
    state: T,
}
impl<T: Serialize + DeserializeOwned + LockableState<T>> Cbor for LockedState<T> {}
