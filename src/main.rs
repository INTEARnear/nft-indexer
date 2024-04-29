mod redis_handler;
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
    run_indexer, AutoContinue, BlockIterator, CompleteTransaction, Indexer, IndexerOptions,
    PreprocessTransactionsSettings,
};
use redis_handler::PushToRedisStream;

#[async_trait]
trait NftEventHandler: Send + Sync {
    async fn handle_mint(&mut self, mint: NftMintEvent, context: EventContext);
    async fn handle_transfer(&mut self, transfer: NftTransferEvent, context: EventContext);
    async fn handle_burn(&mut self, burn: NftBurnEvent, context: EventContext);
}

struct NftIndexer<T: NftEventHandler + Send + Sync + 'static>(T);

#[async_trait]
impl<T: NftEventHandler + Send + Sync + 'static> Indexer for NftIndexer<T> {
    type Error = String;

    async fn on_transaction(
        &mut self,
        transaction: &CompleteTransaction,
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
                                self.0.handle_mint(mint, get_context_lazy()).await;
                            }
                        }
                    }
                    if let Ok(transfer_log) = EventLogData::<NftTransferLog>::deserialize(log) {
                        if transfer_log.validate() {
                            log::debug!("Transfer log: {transfer_log:?}");
                            for transfer in transfer_log.data.0 {
                                self.0.handle_transfer(transfer, get_context_lazy()).await;
                            }
                        }
                    }
                    if let Ok(burn_log) = EventLogData::<NftBurnLog>::deserialize(log) {
                        if burn_log.validate() {
                            log::debug!("Burn log: {burn_log:?}");
                            for burn in burn_log.data.0 {
                                self.0.handle_burn(burn, get_context_lazy()).await;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
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

    let mut indexer = NftIndexer(PushToRedisStream::new(connection, 10_000));

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
