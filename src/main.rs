#[cfg(test)]
mod tests;

use async_trait::async_trait;
use inindexer::fastnear_data_server::FastNearDataServerProvider;
use inindexer::indexer_utils::{
    EventLogData, NftBurnEvent, NftBurnLog, NftMintEvent, NftMintLog, NftTransferEvent,
    NftTransferLog,
};
use inindexer::near_indexer_primitives::types::AccountId;
use inindexer::near_indexer_primitives::CryptoHash;
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::{
    run_indexer, AutoContinue, BlockIterator, CompletedTransaction, Indexer, IndexerOptions,
    PreprocessTransactionsSettings,
};
use redis::aio::MultiplexedConnection;
use redis::streams::StreamMaxlen;
use redis::AsyncCommands;

#[async_trait]
trait NftEventHandler: Send + Sync {
    async fn handle_mint(&mut self, mint: NftMintEvent, context: EventContext);
    async fn handle_transfer(&mut self, transfer: NftTransferEvent, context: EventContext);
    async fn handle_burn(&mut self, burn: NftBurnEvent, context: EventContext);
}

struct NftIndexer<T: NftEventHandler + Send + Sync + 'static> {
    handler: T,
}

#[async_trait]
impl<T: NftEventHandler + Send + Sync + 'static> Indexer for NftIndexer<T> {
    type Error = String;

    async fn on_transaction(
        &mut self,
        transaction: &CompletedTransaction,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        for receipt in transaction.receipts.iter() {
            let get_context_lazy = || {
                let sender_id = receipt.receipt.receipt.predecessor_id.clone();
                let contract_id = receipt.receipt.receipt.receiver_id.clone();
                let txid = transaction.transaction.transaction.hash;
                let block_height = receipt.block_height;
                EventContext {
                    txid,
                    block_height,
                    sender_id,
                    contract_id,
                }
            };
            if receipt.is_successful(false) {
                for log in &receipt.receipt.execution_outcome.outcome.logs {
                    if !log.contains("nep171") {
                        // Don't even start parsing logs if they don't even contain the NEP-171 standard
                        continue;
                    }
                    if let Ok(mint_log) = EventLogData::<NftMintLog>::deserialize(log) {
                        if mint_log.validate() {
                            log::debug!("Mint log: {mint_log:?}");
                            for mint in mint_log.data.0 {
                                self.handler.handle_mint(mint, get_context_lazy()).await;
                            }
                        }
                    }
                    if let Ok(transfer_log) = EventLogData::<NftTransferLog>::deserialize(log) {
                        if transfer_log.validate() {
                            log::debug!("Transfer log: {transfer_log:?}");
                            for transfer in transfer_log.data.0 {
                                self.handler
                                    .handle_transfer(transfer, get_context_lazy())
                                    .await;
                            }
                        }
                    }
                    if let Ok(burn_log) = EventLogData::<NftBurnLog>::deserialize(log) {
                        if burn_log.validate() {
                            log::debug!("Burn log: {burn_log:?}");
                            for burn in burn_log.data.0 {
                                self.handler.handle_burn(burn, get_context_lazy()).await;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

struct PushToRedisStream {
    connection: MultiplexedConnection,
    max_blocks: usize,
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

#[derive(Clone, Debug, PartialEq)]
struct EventContext {
    pub txid: CryptoHash,
    pub block_height: u64,
    pub sender_id: AccountId,
    pub contract_id: AccountId,
}

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()
        .unwrap();

    let client = redis::Client::open(
        std::env::var("REDIS_URL").expect("No $REDIS_URL environment variable set"),
    )
    .unwrap();
    let connection = client.get_multiplexed_tokio_connection().await.unwrap();

    let mut indexer = NftIndexer {
        handler: PushToRedisStream {
            connection,
            max_blocks: 10_000,
        },
    };

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: if std::env::args().len() > 1 {
                // For debugging
                let msg = "Usage: `nft_indexer` or `nft_indexer [start-block] [end-block]`";
                BlockIterator::iterator(
                    std::env::args()
                        .nth(1)
                        .expect(msg)
                        .replace(['_', ',', ' ', '.'], "")
                        .parse()
                        .expect(msg)
                        ..=std::env::args()
                            .nth(2)
                            .expect(msg)
                            .replace(['_', ',', ' ', '.'], "")
                            .parse()
                            .expect(msg),
                )
            } else {
                BlockIterator::AutoContinue(AutoContinue::default())
            },
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: if cfg!(debug_assertions) { 0 } else { 100 },
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .expect("Indexer run failed");
}