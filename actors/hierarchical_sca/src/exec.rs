use crate::tcid::{TAmt, TCid, THamt, TLink};
use crate::{atomic, resolve_secp_bls, StorableMsg};
use cid::Cid;
use fil_actors_runtime::{runtime::Runtime, ActorDowncast};
use fvm_ipld_blockstore::{Blockstore, MemoryBlockstore};
use fvm_ipld_encoding::repr::*;
use fvm_ipld_encoding::{tuple::*, Cbor};
use fvm_shared::address::{Address, SubnetID};
use std::{collections::HashMap, str::FromStr};

#[derive(PartialEq, Eq, Clone, Copy, Debug, Deserialize_repr, Serialize_repr)]
#[repr(u64)]
pub enum ExecStatus {
    UndefState,
    Initialized,
    Success,
    Aborted,
}

#[derive(Clone, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct AtomicExec {
    pub params: AtomicExecParams,
    pub submitted: HashMap<String, Cid>,
    pub status: ExecStatus,
}
impl Cbor for AtomicExec {}

impl AtomicExec {
    pub fn new(params: AtomicExecParams) -> Self {
        AtomicExec {
            params,
            submitted: HashMap::<String, Cid>::new(),
            status: ExecStatus::Initialized,
        }
    }
}

#[derive(PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct SubmitExecParams {
    pub cid: Cid,
    pub abort: bool,
    pub output: atomic::SerializedState, // TODO: LockedState
}
impl Cbor for SubmitExecParams {}

#[derive(Clone, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct AtomicExecParams {
    pub msgs: Vec<StorableMsg>,
    pub inputs: HashMap<String, LockedStateInfo>,
}
impl Cbor for AtomicExecParams {}

/// Output of the initialization of an atomic execution.
// FIXME: Can we probably return the CID directly without
// wrapping it in an object (check Go interop)
#[derive(PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct LockedOutput {
    pub cid: Cid,
}
impl Cbor for LockedOutput {}

/// Output for the submission of an atomic execution.
/// It returns the state of the atomic execution after the submission.
#[derive(PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct SubmitOutput {
    pub status: ExecStatus,
}
impl Cbor for SubmitOutput {}

/// Information to identify the locked state from an actor that is running an atomic
/// execution. To locate some LockedState in a subnet the Cid of the locked state
/// and the actor where it's been locked needs to be specified.
#[derive(Clone, PartialEq, Eq, Serialize_tuple, Deserialize_tuple)]
pub struct LockedStateInfo {
    pub cid: Cid,
    pub actor: Address,
}
impl Cbor for LockedStateInfo {}

#[derive(PartialEq, Eq, Clone, Serialize_tuple, Deserialize_tuple)]
pub struct MetaExec {
    pub msgs_cid: TCid<TAmt<StorableMsg>>,
    pub input_cid: TCid<THamt<Address, LockedStateInfo>>,
}
impl Cbor for MetaExec {}

impl MetaExec {
    pub fn new<BS: Blockstore>(store: &BS) -> anyhow::Result<MetaExec> {
        Ok(Self { msgs_cid: TCid::new_amt(store)?, input_cid: TCid::new_hamt(store)? })
    }
}

impl AtomicExecParams {
    /// translate input addresses into ID address in the current subnet.
    /// The parameters of the atomic execution include non-ID addresses (i.e. keys)
    /// and they need to be translated to their corresponding ID addresses in the
    /// current subnet.
    pub fn input_into_ids<BS, RT>(&mut self, rt: &mut RT) -> anyhow::Result<()>
    where
        BS: Blockstore,
        RT: Runtime<BS>,
    {
        let mut out = HashMap::<String, LockedStateInfo>::new();
        for (key, val) in self.inputs.iter() {
            let addr = Address::from_str(&key)?;
            let sn = addr.subnet()?;
            let addr = addr.raw_addr()?;
            let id_addr = resolve_secp_bls(rt, addr)?;
            // Update with id_addr and subnet
            let sn_addr = Address::new_hierarchical(&sn, &id_addr)?;
            out.insert(sn_addr.to_string(), (*val).clone());
        }
        self.inputs = out;
        Ok(())
    }

    /// Computes the CID for the atomic execution parameters. The input parameters
    /// for the execution determines the CID used to uniquely identify the execution.
    pub fn cid(&self) -> anyhow::Result<Cid> {
        let store = MemoryBlockstore::new();
        let mut meta = MetaExec::new(&store)?;

        meta.msgs_cid.update(&store, |msgs_array| {
            msgs_array.batch_set(self.msgs.clone()).map_err(|e| e.into())
        })?;

        for (k, v) in self.inputs.iter() {
            meta.input_cid.update(&store, |input_map| {
                let addr = Address::from_str(k)?;
                input_map.set(addr.to_bytes().into(), v.clone()).map_err(|e| {
                    e.downcast_wrap(format!("failed to set input map to compute exec cid"))
                })?;
                Ok(())
            })?;
        }

        let meta_cid: TCid<TLink<MetaExec>> = TCid::new_link(&store, &meta)?;

        Ok(meta_cid.cid())
    }
}

/// Computes the common parent for the inputs of the atomic execution.
pub fn is_common_parent(
    curr: &SubnetID,
    inputs: &HashMap<String, LockedStateInfo>,
) -> anyhow::Result<bool> {
    let ks: Vec<String> = inputs.clone().into_keys().collect();
    let addr = Address::from_str(ks[0].as_str())?;
    let mut cp = addr.subnet()?;

    for k in ks.iter() {
        let addr = Address::from_str(k.as_str())?;
        let sn = addr.subnet()?;
        cp = match cp.common_parent(&sn) {
            Some((_, s)) => s,
            None => continue,
        };
    }

    Ok(&cp == curr)
}

/// Check if the address is involved in the execution
pub fn is_addr_in_exec(
    caller: &Address,
    inputs: &HashMap<String, LockedStateInfo>,
) -> anyhow::Result<bool> {
    let ks: Vec<String> = inputs.clone().into_keys().collect();

    for k in ks.iter() {
        let addr = Address::from_str(k.as_str())?;
        let addr = addr.raw_addr()?;

        // if the raw address is equal to caller
        if caller == &addr {
            return Ok(true);
        }
    }
    Ok(false)
}
