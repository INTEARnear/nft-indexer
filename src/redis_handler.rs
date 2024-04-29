use async_trait::async_trait;
use inindexer::indexer_utils::{NftBurnEvent, NftMintEvent, NftTransferEvent};
use redis::{aio::MultiplexedConnection, streams::StreamMaxlen, AsyncCommands};

use crate::{EventContext, NftEventHandler};

pub struct PushToRedisStream {
    connection: MultiplexedConnection,
    max_blocks: usize,
}

impl PushToRedisStream {
    pub fn new(connection: MultiplexedConnection, max_blocks: usize) -> Self {
        Self {
            connection,
            max_blocks,
        }
    }
}

#[async_trait]
impl NftEventHandler for PushToRedisStream {
    async fn handle_mint(&mut self, mint: NftMintEvent, context: EventContext) {
        let response: String = self
            .connection
            .xadd_maxlen(
                "nft_mint",
                StreamMaxlen::Approx(self.max_blocks),
                &format!("{}-*", context.block_height),
                &[
                    ("owner_id", mint.owner_id.as_str()),
                    ("token_ids", mint.token_ids.join(",").as_str()),
                    ("memo", mint.memo.as_deref().unwrap_or("")),
                    ("txid", context.txid.to_string().as_str()),
                    ("block_height", context.block_height.to_string().as_str()),
                    ("sender_id", context.sender_id.as_str()),
                    ("contract_id", context.contract_id.as_str()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }

    async fn handle_transfer(&mut self, transfer: NftTransferEvent, context: EventContext) {
        let response: String = self
            .connection
            .xadd_maxlen(
                "nft_transfer",
                StreamMaxlen::Approx(self.max_blocks),
                &format!("{}-*", context.block_height),
                &[
                    ("old_owner_id", transfer.old_owner_id.as_str()),
                    ("new_owner_id", transfer.new_owner_id.as_str()),
                    ("token_ids", transfer.token_ids.join(",").as_str()),
                    ("memo", transfer.memo.as_deref().unwrap_or("")),
                    ("txid", context.txid.to_string().as_str()),
                    ("block_height", context.block_height.to_string().as_str()),
                    ("sender_id", context.sender_id.as_str()),
                    ("contract_id", context.contract_id.as_str()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }

    async fn handle_burn(&mut self, burn: NftBurnEvent, context: EventContext) {
        let response: String = self
            .connection
            .xadd_maxlen(
                "nft_burn",
                StreamMaxlen::Approx(self.max_blocks),
                &format!("{}-*", context.block_height),
                &[
                    ("owner_id", burn.owner_id.as_str()),
                    ("token_ids", burn.token_ids.join(",").as_str()),
                    ("memo", burn.memo.as_deref().unwrap_or("")),
                    ("txid", context.txid.to_string().as_str()),
                    ("block_height", context.block_height.to_string().as_str()),
                    ("sender_id", context.sender_id.as_str()),
                    ("contract_id", context.contract_id.as_str()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }
}
