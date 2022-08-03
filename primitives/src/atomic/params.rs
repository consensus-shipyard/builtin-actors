use anyhow::anyhow;
use cid::Cid;
use fil_actors_runtime::{runtime::Runtime, ActorDowncast};
use fvm_ipld_blockstore::{Blockstore, MemoryBlockstore};
use fvm_ipld_encoding::repr::*;
use fvm_ipld_encoding::{tuple::*, Cbor};
use fvm_shared::address::{Address, SubnetID};
use std::convert::TryFrom;
use std::{collections::HashMap, str::FromStr};

use crate::taddress::{Hierarchical, TAddress, TAddressKey, ID};
use crate::tcid::{TAmt, TCid, THamt, TLink};
use crate::types::StorableMsg;

/// Status of an atomic execution
#[derive(PartialEq, Eq, Clone, Copy, Debug, Deserialize_repr, Serialize_repr)]
#[repr(u64)]
pub enum ExecStatus {
    /// The atomic execution is initialized and waiting for the submission
    /// of output states
    Initialized = 1,
    /// The execution succeeded.
    Success = 2,
    /// The execution was aborted.
    Aborted = 3,
}

/// Data persisted in the SCA for the orchestration of atomic executions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct AtomicExec {
    /// Parameters of the atomic execution. These parameters also determine
    /// the unique ID for the execution.
    params: AtomicExecParams,
    /// Map that tracks the output submitted by every party involved in the
    /// execution.
    submitted: HashMap<StringifiedAddr, Cid>,
    /// Status of the execution.
    status: ExecStatus,
}
impl Cbor for AtomicExec {}

/// The serialization of Address doesn't support
/// undefined addresses. To overcome this problem
/// in order to be able to use addresses as keys of a hashmap
/// we use their string format (thus this type).
type StringifiedAddr = String;

/// A hierarchical address resolved to an ID.
pub type HierarchicalId = TAddressKey<Hierarchical<ID>>;

impl AtomicExec {
    pub fn new(params: AtomicExecParams) -> Self {
        AtomicExec {
            params,
            submitted: HashMap::<StringifiedAddr, Cid>::new(),
            status: ExecStatus::Initialized,
        }
    }
    pub fn status(&self) -> ExecStatus {
        self.status
    }

    pub fn submitted(&self) -> &HashMap<StringifiedAddr, Cid> {
        &self.submitted
    }

    pub fn submitted_mut(&mut self) -> &mut HashMap<StringifiedAddr, Cid> {
        &mut self.submitted
    }

    pub fn params(&self) -> &AtomicExecParams {
        &self.params
    }

    pub fn set_status(&mut self, st: ExecStatus) {
        self.status = st;
    }
}

/// Parameters used to submit the result of an atomic execution.
#[derive(Clone, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct SubmitExecParams {
    /// Cid of the atomic execution for which a submission want to be sent.
    pub exec_cid: Cid,
    /// Cid of the output of the execution.
    pub output_cid: Cid,
    /// Cid of the the locked state linked to the execution
    /// This is the cid of (exec_cid, lock_cid).
    pub locked_cid: Cid,
}
impl Cbor for SubmitExecParams {}

impl SubmitExecParams {
    // /// Verifies that the locked state has been linked successfully
    // /// to the atomic execution
    // fn verify_locked_cid(
    //     &self,
    //     caller: &TAddress<Hierarchical<ID>>,
    //     exec: &AtomicExec,
    // ) -> anyhow::Result<bool> {
    //     match exec.params().inputs.get(caller.raw_addr()) {
    //         Some(input) => {}
    //         None => return Err(anyhow!("input for address caller not found")),
    //     }
    //     self.locked_cid == exec.cid()
    // }
}

/// Parameters used to abort an atomic execution
#[derive(Clone, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct AbortExecParams {
    /// Cid of the atomic execution for which a submission want to be aborted
    pub exec_cid: Cid,
}

impl Cbor for AbortExecParams {}

/// Parameters to uniquely initiate an atomic execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct AtomicExecParamsRaw {
    pub msgs: Vec<StorableMsg>,
    pub inputs: HashMap<StringifiedAddr, LockedStateInfo>,
}
impl Cbor for AtomicExecParamsRaw {}

/// Parameters to uniquely identify and describe an atomic execution.
///
/// The unique ID of an execution is determined by the CID of its parameters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct AtomicExecParams {
    pub msgs: Vec<StorableMsg>,
    pub inputs: HashMap<HierarchicalId, LockedStateInfo>,
}

/// Output of the initialization of an atomic execution.
// FIXME: Can we probably return the CID directly without
// wrapping it in an object (check Go interop)
#[derive(Debug, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct LockedOutput {
    pub cid: Cid,
}
impl Cbor for LockedOutput {}

/// Output for the submission of an atomic execution.
/// It returns the state of the atomic execution after the submission.
#[derive(Debug, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct SubmitOutput {
    pub status: ExecStatus,
}
impl Cbor for SubmitOutput {}

/// Information to identify the locked state from an actor that is running an atomic
/// execution. To locate some LockedState in a subnet the Cid of the locked state
/// and the actor where it's been locked needs to be specified.
#[derive(Clone, Debug, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct LockedStateInfo {
    /// Cid of the version of the actor state with all the locked
    /// state being used for the execution
    pub cid: Cid,
    /// Address of the actor involved in the execution in the subnet.
    pub actor: Address,
}
impl Cbor for LockedStateInfo {}

#[derive(PartialEq, Eq, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct AtomicExecParamsMeta {
    pub msgs_cid: TCid<TAmt<StorableMsg>>,
    pub inputs_cid: TCid<THamt<Address, LockedStateInfo>>,
}
impl Cbor for AtomicExecParamsMeta {}

impl AtomicExecParamsMeta {
    pub fn new<BS: Blockstore>(store: &BS) -> anyhow::Result<AtomicExecParamsMeta> {
        Ok(Self { msgs_cid: TCid::new_amt(store)?, inputs_cid: TCid::new_hamt(store)? })
    }
}

impl AtomicExecParamsRaw {
    /// translate input addresses into ID address in the current subnet.
    /// The parameters of the atomic execution include non-ID addresses (i.e. keys)
    /// and they need to be translated to their corresponding ID addresses in the
    /// current subnet.
    pub fn input_into_ids<BS, RT>(self, rt: &mut RT) -> anyhow::Result<AtomicExecParams>
    where
        BS: Blockstore,
        RT: Runtime<BS>,
    {
        let mut out = HashMap::new();
        for (key, val) in self.inputs.into_iter() {
            let addr = Address::from_str(&key)?;
            let sn = addr.subnet()?;
            let addr = addr.raw_addr()?;
            let id_addr = match rt.resolve_address(&addr) {
                Some(id) => id,
                None => return Err(anyhow!("couldn't resolve id address in exec input")),
            };
            // Update with id_addr and subnet
            let sn_addr = Address::new_hierarchical(&sn, &id_addr)?;
            let addr = TAddressKey(TAddress::try_from(sn_addr)?);
            out.insert(addr, val);
        }
        Ok(AtomicExecParams { msgs: self.msgs, inputs: out })
    }
    /// Computes the CID for the atomic execution parameters. The input parameters
    /// for the execution determines the CID used to uniquely identify the execution.
    pub fn cid(&self) -> anyhow::Result<Cid> {
        let store = MemoryBlockstore::new();
        let mut meta = AtomicExecParamsMeta::new(&store)?;

        meta.msgs_cid.update(&store, |msgs_array| {
            msgs_array.batch_set(self.msgs.clone()).map_err(|e| e.into())
        })?;

        meta.inputs_cid.update(&store, |input_map| {
            for (k, v) in self.inputs.iter() {
                let addr = Address::from_str(k)?;
                input_map.set(addr.to_bytes().into(), v.clone()).map_err(|e| {
                    e.downcast_wrap(format!("failed to set input map to compute exec cid"))
                })?;
            }
            Ok(())
        })?;

        let meta_cid: TCid<TLink<AtomicExecParamsMeta>> = TCid::new_link(&store, &meta)?;

        Ok(meta_cid.cid())
    }
}

/// Computes the common parent for the inputs of the atomic execution.
pub fn is_common_parent(
    curr: &SubnetID,
    inputs: &HashMap<HierarchicalId, LockedStateInfo>,
) -> anyhow::Result<bool> {
    if inputs.len() == 0 {
        return Err(anyhow!("wrong length! no inputs in hashmap"));
    }

    let ks: Vec<_> = inputs.keys().collect();
    let mut cp = ks[0].0.subnet();

    for k in ks.iter() {
        let sn = k.0.subnet();
        cp = match cp.common_parent(&sn) {
            Some((_, s)) => s,
            None => continue,
        };
    }

    Ok(&cp == curr)
}

/// Check if the address is involved in the execution
pub fn is_addr_in_exec(
    caller: &TAddress<ID>,
    inputs: &HashMap<HierarchicalId, LockedStateInfo>,
) -> anyhow::Result<bool> {
    let ks: Vec<_> = inputs.clone().into_keys().collect();

    for k in ks.iter() {
        let addr = k.0.raw_addr();

        // if the raw address is equal to caller
        if caller == &addr {
            return Ok(true);
        }
    }
    Ok(false)
}
