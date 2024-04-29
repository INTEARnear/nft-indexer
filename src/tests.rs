use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::{
    fastnear_data_server::FastNearDataServerProvider,
    indexer_utils::{NftBurnEvent, NftMintEvent, NftTransferEvent},
    near_indexer_primitives::types::AccountId,
    run_indexer, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};

use crate::{EventContext, NftEventHandler, NftIndexer};

#[tokio::test]
async fn detects_mints() {
    struct TestHandler {
        mint_events: HashMap<AccountId, Vec<(NftMintEvent, EventContext)>>,
    }

    #[async_trait]
    impl NftEventHandler for TestHandler {
        async fn handle_mint(&mut self, mint: NftMintEvent, context: EventContext) {
            self.mint_events
                .entry(context.sender_id.clone())
                .or_insert_with(Vec::new)
                .push((mint, context));
        }

        async fn handle_transfer(&mut self, _transfer: NftTransferEvent, _context: EventContext) {}

        async fn handle_burn(&mut self, _burn: NftBurnEvent, _context: EventContext) {}
    }

    let handler = TestHandler {
        mint_events: HashMap::new(),
    };

    let mut indexer = NftIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(117_189_143..=117_189_146),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
            .mint_events
            .get(&"minter1.sharddog.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            NftMintEvent {
                owner_id: "slimedragon.near".parse().unwrap(),
                token_ids: vec!["19:23".to_owned()],
                memo: None
            },
            EventContext {
                txid: "9TkiwECEL4AMsA6KmuhGskkNFT5Mr6ub6YJJAza8vbGs"
                    .parse()
                    .unwrap(),
                block_height: 117189144,
                sender_id: "minter1.sharddog.near".parse().unwrap(),
                contract_id: "claim.sharddog.near".parse().unwrap()
            }
        )]
    );
}

#[tokio::test]
async fn detects_transfers() {
    struct TestHandler {
        transfer_events: HashMap<AccountId, Vec<(NftTransferEvent, EventContext)>>,
    }

    #[async_trait]
    impl NftEventHandler for TestHandler {
        async fn handle_mint(&mut self, _mint: NftMintEvent, _context: EventContext) {}

        async fn handle_transfer(&mut self, transfer: NftTransferEvent, context: EventContext) {
            let entry = self
                .transfer_events
                .entry(context.sender_id.clone())
                .or_insert_with(Vec::new);
            entry.push((transfer, context));
        }

        async fn handle_burn(&mut self, _burn: NftBurnEvent, _context: EventContext) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = NftIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(117_487_093..=117_487_095),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
            .transfer_events
            .get(&"slimegirl.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            NftTransferEvent {
                authorized_id: None,
                old_owner_id: "slimegirl.near".parse().unwrap(),
                new_owner_id: "tattothetoo.near".parse().unwrap(),
                token_ids: vec!["504983:1".to_owned()],
                memo: None
            },
            EventContext {
                txid: "95HkmF7ajYPSSJnhsGL7C4k8sF5jmdrp4ciiTcK7xuYr"
                    .parse()
                    .unwrap(),
                block_height: 117_487_094,
                sender_id: "slimegirl.near".parse().unwrap(),
                contract_id: "x.paras.near".parse().unwrap()
            }
        )]
    );
}

#[tokio::test]
async fn detects_burns() {
    struct TestHandler {
        burn_events: HashMap<AccountId, Vec<(NftBurnEvent, EventContext)>>,
    }

    #[async_trait]
    impl NftEventHandler for TestHandler {
        async fn handle_mint(&mut self, _mint: NftMintEvent, _context: EventContext) {}

        async fn handle_transfer(&mut self, _transfer: NftTransferEvent, _context: EventContext) {}

        async fn handle_burn(&mut self, burn: NftBurnEvent, context: EventContext) {
            self.burn_events
                .entry(context.sender_id.clone())
                .or_insert_with(Vec::new)
                .push((burn, context));
        }
    }

    let handler = TestHandler {
        burn_events: HashMap::new(),
    };

    let mut indexer = NftIndexer(handler);

    run_indexer(
        &mut indexer,
        FastNearDataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(117_752_571..=117_752_573),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
            .burn_events
            .get(&"bonehedz.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            NftBurnEvent {
                owner_id: "bonehedz.near".parse().unwrap(),
                authorized_id: None,
                token_ids: vec!["1454".to_owned()],
                memo: None
            },
            EventContext {
                txid: "9k7kE7PU1YqrAxzdwKw8P3u8eNeazCZpMWStD89XFBpZ"
                    .parse()
                    .unwrap(),
                block_height: 117752572,
                sender_id: "bonehedz.near".parse().unwrap(),
                contract_id: "veganfriends.mintbase1.near".parse().unwrap()
            }
        )]
    );
}
