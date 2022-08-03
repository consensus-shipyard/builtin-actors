use actor_primitives::tcid::TAmt;
use actor_primitives::tcid::TCid;
use actor_primitives::tcid::TLink;
use actor_primitives::types::StorableMsg;
use cid::Cid;
use fvm_ipld_blockstore::Blockstore;
use fvm_ipld_blockstore::MemoryBlockstore;
use fvm_ipld_encoding::tuple::*;
use fvm_ipld_encoding::Cbor;

use crate::checkpoint::CrossMsgMeta;

#[derive(PartialEq, Eq, Clone, Debug, Default, Serialize_tuple, Deserialize_tuple)]
pub struct CrossMsgs {
    pub msgs: Vec<StorableMsg>,
    pub metas: Vec<CrossMsgMeta>,
}
impl Cbor for CrossMsgs {}

#[derive(PartialEq, Eq, Clone, Debug, Serialize_tuple, Deserialize_tuple)]
pub struct MetaTag {
    pub msgs_cid: TCid<TAmt<StorableMsg>>,
    pub meta_cid: TCid<TAmt<CrossMsgMeta>>,
}
impl Cbor for MetaTag {}

impl MetaTag {
    pub fn new<BS: Blockstore>(store: &BS) -> anyhow::Result<MetaTag> {
        Ok(Self { msgs_cid: TCid::new_amt(store)?, meta_cid: TCid::new_amt(store)? })
    }
}

impl CrossMsgs {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn cid(&self) -> anyhow::Result<Cid> {
        let store = MemoryBlockstore::new();
        let mut meta = MetaTag::new(&store)?;

        meta.msgs_cid.update(&store, |msgs_array| {
            msgs_array.batch_set(self.msgs.clone()).map_err(|e| e.into())
        })?;

        meta.meta_cid.update(&store, |meta_array| {
            meta_array.batch_set(self.metas.clone()).map_err(|e| e.into())
        })?;

        let meta_cid: TCid<TLink<MetaTag>> = TCid::new_link(&store, &meta)?;

        Ok(meta_cid.cid())
    }

    pub(crate) fn add_metas(&mut self, metas: Vec<CrossMsgMeta>) -> anyhow::Result<()> {
        for m in metas.iter() {
            if self.metas.iter().any(|ms| ms == m) {
                continue;
            }
            self.metas.push(m.clone());
        }

        Ok(())
    }

    pub(crate) fn add_msg(&mut self, msg: &StorableMsg) -> anyhow::Result<()> {
        // TODO: Check if the message has already been added.
        self.msgs.push(msg.clone());
        Ok(())
    }
}
