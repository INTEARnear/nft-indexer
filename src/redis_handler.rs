use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use intear_events::events::nft::{
    nft_burn::NftBurnEventData, nft_mint::NftMintEventData, nft_transfer::NftTransferEventData,
};
use redis::aio::ConnectionManager;

use crate::{
    EventContext, ExtendedNftBurnEvent, ExtendedNftMintEvent, ExtendedNftTransferEvent,
    NftEventHandler,
};

pub struct PushToRedisStream {
    mint_stream: RedisEventStream<NftMintEventData>,
    transfer_stream: RedisEventStream<NftTransferEventData>,
    burn_stream: RedisEventStream<NftBurnEventData>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            mint_stream: RedisEventStream::new(connection.clone(), "nft_mint").await,
            transfer_stream: RedisEventStream::new(connection.clone(), "nft_transfer").await,
            burn_stream: RedisEventStream::new(connection.clone(), "nft_burn").await,
            max_stream_size,
        }
    }
}

#[async_trait]
impl NftEventHandler for PushToRedisStream {
    async fn handle_mint(&mut self, mint: ExtendedNftMintEvent, context: EventContext) {
        self.mint_stream
            .emit_event(
                context.block_height,
                NftMintEventData {
                    owner_id: mint.event.owner_id,
                    token_ids: mint.event.token_ids,
                    memo: mint.event.memo,
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                    contract_id: context.contract_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit mint event");
    }

    async fn handle_transfer(&mut self, transfer: ExtendedNftTransferEvent, context: EventContext) {
        self.transfer_stream
            .emit_event(
                context.block_height,
                NftTransferEventData {
                    old_owner_id: transfer.event.old_owner_id,
                    new_owner_id: transfer.event.new_owner_id,
                    token_ids: transfer.event.token_ids,
                    memo: transfer.event.memo,
                    token_prices_near: transfer.trade.token_prices_near,
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                    contract_id: context.contract_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit transfer event");
    }

    async fn handle_burn(&mut self, burn: ExtendedNftBurnEvent, context: EventContext) {
        self.burn_stream
            .emit_event(
                context.block_height,
                NftBurnEventData {
                    owner_id: burn.event.owner_id,
                    token_ids: burn.event.token_ids,
                    memo: burn.event.memo,
                    transaction_id: context.transaction_id,
                    receipt_id: context.receipt_id,
                    block_height: context.block_height,
                    block_timestamp_nanosec: context.block_timestamp_nanosec,
                    contract_id: context.contract_id,
                },
                self.max_stream_size,
            )
            .await
            .expect("Failed to emit burn event");
    }
}
