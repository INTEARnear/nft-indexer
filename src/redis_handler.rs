use async_trait::async_trait;
use redis::{streams::StreamMaxlen, AsyncCommands};

use crate::{EventContext, ExtendedNftBurnEvent, ExtendedNftMintEvent, ExtendedNftTransferEvent, NftEventHandler};

pub struct PushToRedisStream<C: AsyncCommands + Sync> {
    connection: C,
    max_stream_size: usize,
}

impl<C: AsyncCommands + Sync> PushToRedisStream<C> {
    pub fn new(connection: C, max_stream_size: usize) -> Self {
        Self {
            connection,
            max_stream_size,
        }
    }
}

#[async_trait]
impl<C: AsyncCommands + Sync> NftEventHandler for PushToRedisStream<C> {
    async fn handle_mint(&mut self, mint: ExtendedNftMintEvent, context: EventContext) {
        let response: String = self
            .connection
            .xadd_maxlen(
                "nft_mint",
                StreamMaxlen::Approx(self.max_stream_size),
                format!("{}-*", context.block_height),
                &[
                    ("mint", serde_json::to_string(&mint).unwrap().as_str()),
                    ("context", serde_json::to_string(&context).unwrap().as_str()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }

    async fn handle_transfer(&mut self, transfer: ExtendedNftTransferEvent, context: EventContext) {
        let response: String = self
            .connection
            .xadd_maxlen(
                "nft_transfer",
                StreamMaxlen::Approx(self.max_stream_size),
                format!("{}-*", context.block_height),
                &[
                    (
                        "transfer",
                        serde_json::to_string(&transfer).unwrap().as_str(),
                    ),
                    ("context", serde_json::to_string(&context).unwrap().as_str()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }

    async fn handle_burn(&mut self, burn: ExtendedNftBurnEvent, context: EventContext) {
        let response: String = self
            .connection
            .xadd_maxlen(
                "nft_burn",
                StreamMaxlen::Approx(self.max_stream_size),
                format!("{}-*", context.block_height),
                &[
                    ("burn", serde_json::to_string(&burn).unwrap().as_str()),
                    ("context", serde_json::to_string(&context).unwrap().as_str()),
                ],
            )
            .await
            .unwrap();
        log::debug!("Adding to stream: {response}");
    }
}
