pub mod redis_handler;

use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::types::{AccountId, Balance, BlockHeight};
use inindexer::near_indexer_primitives::views::{ActionView, ExecutionStatusView, ReceiptEnumView};
use inindexer::near_indexer_primitives::CryptoHash;
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::near_utils::{
    dec_format, dec_format_map, EventLogData, NftBurnEvent, NftBurnLog, NftMintEvent, NftMintLog,
    NftTransferEvent, NftTransferLog,
};
use inindexer::{IncompleteTransaction, Indexer, TransactionReceipt};
use serde::Deserialize;

#[async_trait]
pub trait NftEventHandler: Send + Sync {
    async fn handle_mint(&mut self, mint: ExtendedNftMintEvent, context: EventContext);
    async fn handle_transfer(&mut self, transfer: ExtendedNftTransferEvent, context: EventContext);
    async fn handle_burn(&mut self, burn: ExtendedNftBurnEvent, context: EventContext);

    /// Called after each block
    async fn flush_events(&mut self, block_height: BlockHeight);
}

#[derive(Debug, PartialEq)]
pub struct ExtendedNftMintEvent {
    pub event: NftMintEvent,
}

impl ExtendedNftMintEvent {
    pub fn from_event(event: NftMintEvent) -> Self {
        ExtendedNftMintEvent { event }
    }
}

#[derive(Debug, PartialEq)]
pub struct ExtendedNftTransferEvent {
    pub event: NftTransferEvent,
    pub trade: NftTradeDetails,
}

impl ExtendedNftTransferEvent {
    pub fn from_event(event: NftTransferEvent, receipt: &TransactionReceipt) -> Self {
        let mut prices = vec![None; event.token_ids.len()];
        if let ReceiptEnumView::Action { actions, .. } = &receipt.receipt.receipt.receipt {
            for action in actions {
                if let ActionView::FunctionCall {
                    method_name, args, ..
                } = action
                {
                    if method_name == "nft_transfer_payout" {
                        if let ExecutionStatusView::SuccessValue(value) =
                            &receipt.receipt.execution_outcome.outcome.status
                        {
                            if let Ok(args) = serde_json::from_slice::<NftTransferPayoutArgs>(args)
                            {
                                if let Some(index) = event
                                    .token_ids
                                    .iter()
                                    .position(|token_id| **token_id == args.token_id)
                                {
                                    if let Ok(payout) =
                                        serde_json::from_slice::<PayoutResponse>(value)
                                    {
                                        // Is this always the same as args.balance?
                                        let price = payout.payout.values().sum::<Balance>();
                                        prices[index] = Some(price);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        ExtendedNftTransferEvent {
            event,
            trade: NftTradeDetails {
                token_prices_near: prices,
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct NftTradeDetails {
    /// None if it's a simple transfer, Some if it's a trade. Guaranteed to have the same length as NftTransferEvent::token_ids
    pub token_prices_near: Vec<Option<Balance>>,
}

#[derive(Debug, PartialEq)]
pub struct ExtendedNftBurnEvent {
    pub event: NftBurnEvent,
}

impl ExtendedNftBurnEvent {
    pub fn from_event(event: NftBurnEvent) -> Self {
        ExtendedNftBurnEvent { event }
    }
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct NftTransferPayoutArgs {
    receiver_id: AccountId,
    token_id: String,
    #[serde(with = "dec_format")]
    approval_id: Option<u64>,
    memo: Option<String>,
    #[serde(with = "dec_format")]
    balance: Balance,
    max_len_payout: Option<u32>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct PayoutResponse {
    #[serde(with = "dec_format_map")]
    payout: HashMap<AccountId, Balance>,
}

pub struct NftIndexer<T: NftEventHandler + Send + Sync + 'static>(pub T);

#[async_trait]
impl<T: NftEventHandler + Send + Sync + 'static> Indexer for NftIndexer<T> {
    type Error = String;

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        transaction: &IncompleteTransaction,
        _block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        let get_context_lazy = || {
            let tx_sender_id = receipt.receipt.receipt.predecessor_id.clone();
            let contract_id = receipt.receipt.receipt.receiver_id.clone();
            let transaction_id = transaction.transaction.transaction.hash;
            let receipt_id = receipt.receipt.receipt.receipt_id;
            let block_height = receipt.block_height;
            let block_timestamp_nanosec = receipt.block_timestamp_nanosec;
            EventContext {
                transaction_id,
                receipt_id,
                block_height,
                block_timestamp_nanosec,
                tx_sender_id,
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
                            self.0
                                .handle_mint(
                                    ExtendedNftMintEvent::from_event(mint),
                                    get_context_lazy(),
                                )
                                .await;
                        }
                    }
                }
                if let Ok(transfer_log) = EventLogData::<NftTransferLog>::deserialize(log) {
                    if transfer_log.validate() {
                        log::debug!("Transfer log: {transfer_log:?}");
                        for transfer in transfer_log.data.0 {
                            self.0
                                .handle_transfer(
                                    ExtendedNftTransferEvent::from_event(transfer, receipt),
                                    get_context_lazy(),
                                )
                                .await;
                        }
                    }
                }
                if let Ok(burn_log) = EventLogData::<NftBurnLog>::deserialize(log) {
                    if burn_log.validate() {
                        log::debug!("Burn log: {burn_log:?}");
                        for burn in burn_log.data.0 {
                            self.0
                                .handle_burn(
                                    ExtendedNftBurnEvent::from_event(burn),
                                    get_context_lazy(),
                                )
                                .await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_block_end(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        self.0.flush_events(block.block.header.height).await;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EventContext {
    pub transaction_id: CryptoHash,
    pub receipt_id: CryptoHash,
    pub block_height: BlockHeight,
    pub block_timestamp_nanosec: u128,
    pub tx_sender_id: AccountId,
    pub contract_id: AccountId,
}
