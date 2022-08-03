use anyhow::anyhow;

use cid::multihash::Code::Blake2b256;
use cid::multihash::MultihashDigest;
use cid::Cid;
use fvm_ipld_encoding::{tuple::*, Cbor, RawBytes, DAG_CBOR};
use fvm_shared::MethodNum;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

pub mod params;

use crate::{
    tcid::{TCid, THamt},
    types::StorableMsg,
};

use self::params::AtomicExecParamsRaw;

/// Trait that determines the functions that need to be implemented by
/// a state object to be "embeddable" inside a lockable state.
///
/// The implementation should be provided with a default strategy to merge
/// two objects of the same type. Actor developers may choose to use alternative
/// merging strategies in different situations, but the default merging
/// strategy should be provided.
pub trait MergeableState {
    /// Merge a locked state (not necessarily the output) to the current state.
    fn merge(&mut self, other: Self) -> anyhow::Result<()>;
}

/// Trait to be implemented by the state of an actor to support
/// atomic executions.
pub trait LockableActorState
where
    Self: Serialize + DeserializeOwned,
{
    /// Returns an empty instance of the state to be populated
    /// with locked variables.
    fn new() -> Self;

    /// Specifies how to merge two instances of the actor state
    /// according to the messages used for the execution.
    fn merge(&mut self, other: Self, params: LockParams);

    fn cid(&self) -> anyhow::Result<Cid> {
        let ser = RawBytes::serialize(&self)?;
        Ok(Cid::new_v1(DAG_CBOR, Blake2b256.digest(ser.as_slice())))
    }

    /// Gets state object from serialized state.
    fn from_serialized(value: &SerializedState) -> anyhow::Result<Self> {
        Ok(RawBytes::deserialize(&value)?)
    }
}

/// LockableState
pub struct LockableState<T>
where
    T: Serialize + DeserializeOwned + MergeableState,
{
    lock: bool,
    /// CID of the linked execution to the locked state.
    exec_cid: Option<Cid>,
    s: T,
}

impl<T> LockableState<T>
where
    T: Serialize + DeserializeOwned + MergeableState,
{
    /// Creates new lockable state from object
    pub fn new(s: T) -> Self {
        LockableState { lock: false, s, exec_cid: None }
    }

    /// Locks the state
    pub fn lock(&mut self) -> anyhow::Result<()> {
        if self.lock {
            return Err(anyhow!("state already locked"));
        }
        self.lock = true;
        Ok(())
    }

    /// Unlocks the state
    pub fn unlock(&mut self) -> anyhow::Result<()> {
        if !self.lock {
            return Err(anyhow!("state already unlocked"));
        }
        self.lock = false;
        Ok(())
    }

    /// Check if the state is locked
    pub fn is_locked(&self) -> bool {
        return self.lock;
    }

    pub fn state(&self) -> &T {
        &self.s
    }

    pub fn exec_cid(&self) -> Option<Cid> {
        self.exec_cid
    }
}

/// Return type for all actor functions.
///
/// It returns an option for developers to optionally choose if
/// to return an output in the function.
type ActorResult = anyhow::Result<Option<RawBytes>>;

/// Type alias for serialized actor state
// FIXME: Add a generic bound to limit the serialized objects
// that can be used in the LockedMap to the LockableActorState.
type SerializedState = RawBytes;
/// Internal map kept by actor supporting atomic executions to track
/// the states that have been locked and are used in an atomic exec.
pub type LockedMap = TCid<THamt<Cid, SerializedState>>;

/// Method number to call `lock` and prepare (and lock) the state
/// for an atomic execution.
pub const METHOD_LOCK: MethodNum = 2;
/// Method number used to commit the output of an execution in a subnet
/// and trigger its propagation to the common_parent executing the
/// execution.
pub const METHOD_PRE_COMMIT: MethodNum = 3;
/// Method number to trigger the commitment of the output state.
/// This message is called through a top-down message.
pub const METHOD_COMMIT: MethodNum = 5;
/// Method number to signal in the actor that the execution
/// has been aborted and the state can be unlocked. This method
/// is called through a top-down message.
pub const METHOD_ABORT: MethodNum = 4;

/// Trait for an actor able to support an atomic execution.
///
/// The functions of this trait represent the set of methods that
/// and actor support atomic executions needs to implement. Correspondingly,
/// it follows the same return convention used for every FVM actor method.
pub trait LockableActor<S>
where
    S: Serialize + DeserializeOwned + LockableActorState,
{
    /// Function to prepare and locked the state generally for
    /// an atomic execution according to the locking params of
    /// the execution.
    fn lock(params: LockParams) -> ActorResult;

    /// Function to call the pre_comit stage of an execution,
    /// where the locked state is linked to a specific execution,
    /// and the output of the execution is submitted and persisted
    /// to be propagated to the common parent orchestrating the execution.
    fn pre_commit(params: PreCommitParams) -> ActorResult;

    /// Function triggered by a bottom-up message that performs the commitment
    /// of the output state by a specific execution the actor is involved in.
    fn commit(params: LockParams) -> ActorResult;

    /// Function triggered by a bottom-up message that aborts the execution
    /// and unlocks the corresponding state.
    fn abort(exec_cid: Cid) -> ActorResult;

    /// Determines if a specific actor method is supported to run
    /// an atomic execution over it.
    fn is_atomic_method(method: MethodNum) -> bool;

    /// Sets an instance of locked state in the LockedMap
    fn set_locked_state(&mut self) -> anyhow::Result<Cid>;

    /// Gets an instance of locked state in the LockedMap
    fn get_locked_state(&self, cid: Cid) -> Self;
}

/// Parameters used to lock certain state of an actor for its use in an atomic
/// execution
///
/// Different locking strategies may be implemented in the actor according to the
/// method and parameters used in the atomic execution. The parameters gives information
/// about the message that are used in the execution, the return of the locking
/// phase is a list of cids of states that have been merged in the actor's state and
/// that need to be used for the execution.
#[derive(Debug, Eq, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct LockParams {
    msgs: Vec<StorableMsg>,
}
impl Cbor for LockParams {}
impl LockParams {
    pub fn new(msgs: Vec<StorableMsg>) -> Self {
        LockParams { msgs }
    }
}

/// Params for the pre_commit stage where the locked state is linked to a specific
/// execution, and the output of the execution is notified. This triggers and bottom-up
/// message towards the common parent orchestrating the state to initialize (or commit)
/// the output
#[derive(Debug, Eq, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct PreCommitParams {
    /// Parameters agreed by the parties for the atomic execution.
    pub exec: AtomicExecParamsRaw,
    /// SerializedState that represents the output of the execution.
    pub output: SerializedState,
}
impl Cbor for PreCommitParams {}

/// Commit parameters that pass through a top-down message information about
/// the execution to be committed and the cid of the output that needs to be committed.
#[derive(Debug, Eq, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct CommitParams {
    pub exec_cid: Cid,
    pub output_cid: Cid,
}
impl Cbor for CommitParams {}
impl CommitParams {
    pub fn new(exec_cid: Cid, output_cid: Cid) -> Self {
        CommitParams { exec_cid, output_cid }
    }
}

/// Abort parameters that pass through a top-down message to unlock state
/// after an aborted execution.
#[derive(Debug, Eq, PartialEq, Serialize_tuple, Deserialize_tuple)]
pub struct AbortParams {
    pub exec_cid: Cid,
}
impl Cbor for AbortParams {}
impl AbortParams {
    pub fn new(exec_cid: Cid) -> Self {
        AbortParams { exec_cid }
    }
}
