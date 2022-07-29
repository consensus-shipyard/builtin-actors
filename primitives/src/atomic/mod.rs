use anyhow::anyhow;

use cid::multihash::Code::Blake2b256;
use cid::multihash::MultihashDigest;
use cid::Cid;
use fil_actors_runtime::cbor;
use fvm_ipld_encoding::{tuple::*, Cbor, RawBytes, DAG_CBOR};
use fvm_shared::bigint::bigint_ser;
use fvm_shared::{address::Address, econ::TokenAmount, MethodNum};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use crate::tcid::{TCid, THamt};

/// Trait that determines the functions that need to be implemented by
/// a state object to be lockable and be used in an atomic execution.
///
/// Different strategies may be used to merge different locked state to
/// prepare the actor state for the execution, and for the merging of the
/// output of the execution to the original state of the actor.
pub trait MergeableState {
    /// Merge a locked state (not necessarily the output) to the current state.
    fn merge(&mut self, other: Self) -> anyhow::Result<()>;
    /// Merge the output of an execution to the current state.
    fn merge_output(&mut self, other: Self) -> anyhow::Result<()>;
}

/// Internal map kept by actor supporting atomic executions to track
/// the states that have been locked and are used in an atomic exec.
pub type LockedMap = TCid<THamt<Cid, SerializedState>>;

/// StorableMsg stores all the relevant information required
/// to execute cross-messages.
///
/// We follow this approach because we can't directly store types.Message
/// as we did in the actor's Go counter-part. Instead we just persist the
/// information required to create the cross-messages and execute in the
/// corresponding node implementation.
// FIXME: Take StorableMsg from cross.rs and put it here.
#[derive(PartialEq, Eq, Clone, Debug, Serialize_tuple, Deserialize_tuple)]
pub struct StorableMsg {
    pub from: Address,
    pub to: Address,
    pub method: MethodNum,
    pub params: RawBytes,
    #[serde(with = "bigint_ser")]
    pub value: TokenAmount,
    pub nonce: u64,
}

impl Cbor for StorableMsg {}
/// Trait that specifies the interface of an actor state able to support
/// atomic executions.
pub trait LockableActorState {
    /// Map with all the locked state in the actor uniquely identified through
    /// their Cid.
    fn locked_map_cid(&self) -> LockedMap;
}

/// Return type for all actor functions.
///
/// It returns an option for developers to optionally choose if
/// to return an output in the function.
type ActorResult = anyhow::Result<Option<RawBytes>>;

/// Trait for an actor able to support an atomic execution.
///
/// The functions of this trait represent the set of methods that
/// and actor support atomic executions needs to implement. Correspondingly,
/// it follows the same return convention used for every FVM actor method.
pub trait LockableActor<S>
where
    S: Serialize + DeserializeOwned + LockableActorState,
{
    const METHOD_LOCK: MethodNum = 2;
    const METHOD_MERGE: MethodNum = 3;
    const METHOD_ABORT: MethodNum = 4;
    const METHOD_UNLOCK: MethodNum = 5;
    fn lock(params: LockParams) -> ActorResult;
    fn pre_commit() -> ActorResult;
    fn commit(params: LockParams) -> ActorResult;
    fn abort(params: LockParams) -> ActorResult;
    fn state(params: LockParams) -> S;
}

type TypeCode = String;
pub trait StateType {
    fn state_type(&self) -> TypeCode;
}

#[macro_export]
macro_rules! register_types {
    ($($typ:ident),+) => {
        $(
            impl StateType for $typ {
                fn state_type(&self) -> TypeCode {
                    stringify!($typ).to_string()
                }
            }
        )*
    };
}

macro_rules! build {
    ($($body:tt)*) => {
        as_item! {
            enum STypes { $($body)* }
        }
    };
}

macro_rules! as_item {
    ($i:item) => {
        $i
    };
}

impl<T> Into<SerializedState> for State<T>
where
    T: MergeableState + StateType + Serialize + DeserializeOwned,
{
    fn into(self) -> SerializedState {
        SerializedState { t: self.s.state_type(), ser: RawBytes::serialize(self.s).unwrap().into() }
    }
}

/// Serializes exactly as its underlying `Cid`.
impl<T> serde::Serialize for State<T>
where
    T: MergeableState + StateType + Serialize + DeserializeOwned,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerializedState { t: self.s.state_type(), ser: RawBytes::serialize(&self.s).unwrap() }
            .serialize(serializer)
    }
}

impl<'d, T> serde::Deserialize<'d> for State<T>
where
    T: MergeableState + StateType + Serialize + DeserializeOwned,
    Self: TryFrom<SerializedState>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'d>,
    {
        RawBytes::deserialize(&SerializedState::deserialize(deserializer)?.ser)
            .map_err(|e| serde::de::Error::custom(format!("error deserializing state: {}", e)))
    }
}

/// Serialized representation of the locked state of an actor.
#[derive(Debug, PartialEq, Eq, Clone, Serialize_tuple, Deserialize_tuple, Default)]
pub struct SerializedState {
    /// type of the serialized state
    t: TypeCode,
    /// serialization of the state
    // #[serde(with = "serde_bytes")]
    ser: RawBytes,
}

pub struct State<T>
where
    T: MergeableState + StateType + Serialize + DeserializeOwned,
{
    s: T,
}

impl<T> State<T>
where
    T: MergeableState + StateType + Serialize + DeserializeOwned,
{
    pub fn new(s: T) -> Self {
        Self { s }
    }

    pub fn cid(&self) -> anyhow::Result<Cid> {
        let ser = RawBytes::serialize(&self)?;
        Ok(Cid::new_v1(DAG_CBOR, Blake2b256.digest(ser.as_slice())))
    }

    pub fn from_serialized(value: &SerializedState) -> anyhow::Result<Self> {
        // if get_type_code!(T).to_string() != value.t {
        //     return Err(anyhow!("error: serialized state has the wrong type"));
        // }
        Ok(State::new(RawBytes::deserialize(&value.ser)?))
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
    msgs: Vec<StorableMsg>,
}
impl Cbor for LockParams {}
impl LockParams {
    pub fn new(msgs: Vec<StorableMsg>) -> Self {
        LockParams { msgs }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq, Clone, Serialize_tuple, Deserialize_tuple, Default)]
    pub struct TestType {
        pub dummy: u64,
    }
    register_types!(TestType);

    impl MergeableState for TestType {
        fn merge(&mut self, _other: Self) -> anyhow::Result<()> {
            Ok(())
        }
        fn merge_output(&mut self, _other: Self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_types() {
        let a = TestType { dummy: 1 };
        assert_eq!(a.state_type(), "TestType");
        let st = State::new(a);
        let ser: SerializedState = st.into();
        State::<TestType>::from_serialized(&ser).unwrap();
    }
}
