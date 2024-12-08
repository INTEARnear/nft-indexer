use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::BlockHeight;
use intear_events::events::nft::{
    nft_burn::NftBurnEvent, nft_mint::NftMintEvent, nft_transfer::NftTransferEvent,
};
use redis::aio::ConnectionManager;

use crate::{
    EventContext, ExtendedNftBurnEvent, ExtendedNftMintEvent, ExtendedNftTransferEvent,
    NftEventHandler,
};

pub struct PushToRedisStream {
    mint_stream: RedisEventStream<NftMintEvent>,
    transfer_stream: RedisEventStream<NftTransferEvent>,
    burn_stream: RedisEventStream<NftBurnEvent>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            mint_stream: RedisEventStream::new(connection.clone(), "nft_mint"),
            transfer_stream: RedisEventStream::new(connection.clone(), "nft_transfer"),
            burn_stream: RedisEventStream::new(connection.clone(), "nft_burn"),
            max_stream_size,
        }
    }
}

#[async_trait]
impl NftEventHandler for PushToRedisStream {
    async fn handle_mint(&mut self, mint: ExtendedNftMintEvent, context: EventContext) {
        self.mint_stream.add_event(NftMintEvent {
            owner_id: mint.event.owner_id,
            token_ids: mint.event.token_ids,
            memo: mint.event.memo,
            transaction_id: context.transaction_id,
            receipt_id: context.receipt_id,
            block_height: context.block_height,
            block_timestamp_nanosec: context.block_timestamp_nanosec,
            contract_id: context.contract_id,
        });
    }

    async fn handle_transfer(&mut self, transfer: ExtendedNftTransferEvent, context: EventContext) {
        self.transfer_stream.add_event(NftTransferEvent {
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
        });
    }

    async fn handle_burn(&mut self, burn: ExtendedNftBurnEvent, context: EventContext) {
        self.burn_stream.add_event(NftBurnEvent {
            owner_id: burn.event.owner_id,
            token_ids: burn.event.token_ids,
            memo: burn.event.memo,
            transaction_id: context.transaction_id,
            receipt_id: context.receipt_id,
            block_height: context.block_height,
            block_timestamp_nanosec: context.block_timestamp_nanosec,
            contract_id: context.contract_id,
        });
    }

    async fn flush_events(&mut self, block_height: BlockHeight) {
        self.mint_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush mint stream");
        self.transfer_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush transfer stream");
        self.burn_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush burn stream");
    }
}
