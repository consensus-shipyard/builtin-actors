use anyhow::anyhow;
use cid::Cid;
use fil_actors_runtime::BURNT_FUNDS_ACTOR_ADDR;
use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_blockstore::MemoryBlockstore;
use fvm_ipld_encoding::tuple::*;
use fvm_ipld_encoding::Cbor;
use fvm_ipld_encoding::RawBytes;
use fvm_shared::address::{Address, SubnetID};
use fvm_shared::bigint::bigint_ser;
use fvm_shared::econ::TokenAmount;
use fvm_shared::MethodNum;
use fvm_shared::METHOD_SEND;
use std::path::Path;

/// StorableMsg stores all the relevant information required
/// to execute cross-messages.
///
/// We follow this approach because we can't directly store types.Message
/// as we did in the actor's Go counter-part. Instead we just persist the
/// information required to create the cross-messages and execute in the
/// corresponding node implementation.
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

impl Default for StorableMsg {
    fn default() -> Self {
        Self {
            from: Address::new_id(0),
            to: Address::new_id(0),
            method: 0,
            params: RawBytes::default(),
            value: TokenAmount::from(0),
            nonce: 0,
        }
    }
}

#[derive(PartialEq, Eq)]
pub enum HCMsgType {
    Unknown = 0,
    BottomUp,
    TopDown,
}

impl StorableMsg {
    pub fn new_release_msg(
        sub_id: &SubnetID,
        sig_addr: &Address,
        value: TokenAmount,
        nonce: u64,
    ) -> anyhow::Result<Self> {
        let to = Address::new_hierarchical(
            &match sub_id.parent() {
                Some(s) => s,
                None => return Err(anyhow!("error getting parent for subnet addr")),
            },
            sig_addr,
        )?;
        let from = Address::new_hierarchical(sub_id, &BURNT_FUNDS_ACTOR_ADDR)?;
        Ok(Self { from, to, method: METHOD_SEND, params: RawBytes::default(), value, nonce })
    }

    pub fn new_fund_msg(
        sub_id: &SubnetID,
        sig_addr: &Address,
        value: TokenAmount,
    ) -> anyhow::Result<Self> {
        let from = Address::new_hierarchical(
            &match sub_id.parent() {
                Some(s) => s,
                None => return Err(anyhow!("error getting parent for subnet addr")),
            },
            sig_addr,
        )?;
        let to = Address::new_hierarchical(sub_id, sig_addr)?;
        // the nonce and the rest of message fields are set when the message is committed.
        Ok(Self { from, to, method: METHOD_SEND, value, ..Default::default() })
    }

    pub fn hc_type(&self) -> anyhow::Result<HCMsgType> {
        let sto = self.to.subnet()?;
        let sfrom = self.from.subnet()?;
        if is_bottomup(&sfrom, &sto) {
            return Ok(HCMsgType::BottomUp);
        }
        Ok(HCMsgType::TopDown)
    }

    pub fn apply_type(&self, curr: &SubnetID) -> anyhow::Result<HCMsgType> {
        let sto = self.to.subnet()?;
        let sfrom = self.from.subnet()?;
        if curr.common_parent(&sto) == sfrom.common_parent(&sto)
            && self.hc_type()? == HCMsgType::BottomUp
        {
            return Ok(HCMsgType::BottomUp);
        }
        Ok(HCMsgType::TopDown)
    }
}

pub fn is_bottomup(from: &SubnetID, to: &SubnetID) -> bool {
    let index = match from.common_parent(&to) {
        Some((ind, _)) => ind,
        None => return false,
    };
    let a = from.to_string();
    Path::new(&a).components().count() - 1 > index
}

#[cfg(test)]
mod tests {
    use crate::cross::*;
    use std::str::FromStr;

    #[test]
    fn test_is_bottomup() {
        bottom_up("/root/f01", "/root/f01/f02", false);
        bottom_up("/root/f01", "/root", true);
        bottom_up("/root/f01", "/root/f01/f02", false);
        bottom_up("/root/f01", "/root/f02/f02", true);
        bottom_up("/root/f01/f02", "/root/f01/f02", false);
        bottom_up("/root/f01/f02", "/root/f01/f02/f03", false);
    }
    fn bottom_up(a: &str, b: &str, res: bool) {
        assert_eq!(
            is_bottomup(&SubnetID::from_str(a).unwrap(), &SubnetID::from_str(b).unwrap()),
            res
        );
    }
}
