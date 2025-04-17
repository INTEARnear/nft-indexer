use std::collections::HashMap;

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::{AccountId, BlockHeight},
    near_utils::{NftBurnEvent, NftMintEvent, NftTransferEvent},
    neardata::NeardataProvider,
    run_indexer, BlockRange, IndexerOptions, PreprocessTransactionsSettings,
};

use nft_indexer::{
    EventContext, ExtendedNftBurnEvent, ExtendedNftMintEvent, ExtendedNftTransferEvent,
    NftEventHandler, NftIndexer, NftTradeDetails,
};

#[tokio::test]
async fn detects_mints() {
    struct TestHandler {
        mint_events: HashMap<AccountId, Vec<(ExtendedNftMintEvent, EventContext)>>,
    }

    #[async_trait]
    impl NftEventHandler for TestHandler {
        async fn handle_mint(&mut self, mint: ExtendedNftMintEvent, context: EventContext) {
            self.mint_events
                .entry(context.tx_sender_id.clone())
                .or_insert_with(Vec::new)
                .push((mint, context));
        }

        async fn handle_transfer(
            &mut self,
            _transfer: ExtendedNftTransferEvent,
            _context: EventContext,
        ) {
        }

        async fn handle_burn(&mut self, _burn: ExtendedNftBurnEvent, _context: EventContext) {}

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        mint_events: HashMap::new(),
    };

    let mut indexer = NftIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 117_189_143,
                end_exclusive: Some(117_189_146),
            })
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
            ExtendedNftMintEvent {
                event: NftMintEvent {
                    owner_id: "slimedragon.near".parse().unwrap(),
                    token_ids: vec!["19:23".to_owned()],
                    memo: None
                }
            },
            EventContext {
                transaction_id: "9TkiwECEL4AMsA6KmuhGskkNFT5Mr6ub6YJJAza8vbGs"
                    .parse()
                    .unwrap(),
                receipt_id: "DrrW649B53RQaejPgRqiKM74MyT35JPk9cbkokkUGKdf"
                    .parse()
                    .unwrap(),
                block_height: 117189144,
                block_timestamp_nanosec: 1713553179034135476,
                tx_sender_id: "minter1.sharddog.near".parse().unwrap(),
                contract_id: "claim.sharddog.near".parse().unwrap()
            }
        )]
    );
}

#[tokio::test]
async fn detects_transfers() {
    struct TestHandler {
        transfer_events: HashMap<AccountId, Vec<(ExtendedNftTransferEvent, EventContext)>>,
    }

    #[async_trait]
    impl NftEventHandler for TestHandler {
        async fn handle_mint(&mut self, _mint: ExtendedNftMintEvent, _context: EventContext) {}

        async fn handle_transfer(
            &mut self,
            transfer: ExtendedNftTransferEvent,
            context: EventContext,
        ) {
            let entry = self
                .transfer_events
                .entry(context.tx_sender_id.clone())
                .or_insert_with(Vec::new);
            entry.push((transfer, context));
        }

        async fn handle_burn(&mut self, _burn: ExtendedNftBurnEvent, _context: EventContext) {}

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = NftIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 117_487_093,
                end_exclusive: Some(117_487_095),
            })
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
            ExtendedNftTransferEvent {
                event: NftTransferEvent {
                    authorized_id: None,
                    old_owner_id: "slimegirl.near".parse().unwrap(),
                    new_owner_id: "tattothetoo.near".parse().unwrap(),
                    token_ids: vec!["504983:1".to_owned()],
                    memo: None
                },
                trade: NftTradeDetails {
                    token_prices_near: vec![None],
                }
            },
            EventContext {
                transaction_id: "95HkmF7ajYPSSJnhsGL7C4k8sF5jmdrp4ciiTcK7xuYr"
                    .parse()
                    .unwrap(),
                receipt_id: "AhbWgoat1L23YgrzrWE6U2FcM1n5uqRZ8cKxkxevdFJa"
                    .parse()
                    .unwrap(),
                block_height: 117_487_094,
                block_timestamp_nanosec: 1713920604063293990,
                tx_sender_id: "slimegirl.near".parse().unwrap(),
                contract_id: "x.paras.near".parse().unwrap()
            }
        )]
    );
}

#[tokio::test]
async fn detects_burns() {
    struct TestHandler {
        burn_events: HashMap<AccountId, Vec<(ExtendedNftBurnEvent, EventContext)>>,
    }

    #[async_trait]
    impl NftEventHandler for TestHandler {
        async fn handle_mint(&mut self, _mint: ExtendedNftMintEvent, _context: EventContext) {}

        async fn handle_transfer(
            &mut self,
            _transfer: ExtendedNftTransferEvent,
            _context: EventContext,
        ) {
        }

        async fn handle_burn(&mut self, burn: ExtendedNftBurnEvent, context: EventContext) {
            self.burn_events
                .entry(context.tx_sender_id.clone())
                .or_insert_with(Vec::new)
                .push((burn, context));
        }

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        burn_events: HashMap::new(),
    };

    let mut indexer = NftIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 117_752_571,
                end_exclusive: Some(117_752_573),
            })
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
            ExtendedNftBurnEvent {
                event: NftBurnEvent {
                    owner_id: "bonehedz.near".parse().unwrap(),
                    authorized_id: None,
                    token_ids: vec!["1454".to_owned()],
                    memo: None
                }
            },
            EventContext {
                transaction_id: "9k7kE7PU1YqrAxzdwKw8P3u8eNeazCZpMWStD89XFBpZ"
                    .parse()
                    .unwrap(),
                receipt_id: "4EVVVu8VR72Gd4cfhxworayV1CuA29DL9ndE7KfdRcKN"
                    .parse()
                    .unwrap(),
                block_height: 117752572,
                block_timestamp_nanosec: 1714240014556084087,
                tx_sender_id: "bonehedz.near".parse().unwrap(),
                contract_id: "veganfriends.mintbase1.near".parse().unwrap()
            }
        )]
    );
}

#[tokio::test]
async fn detects_paras_trade() {
    struct TestHandler {
        transfer_events: HashMap<AccountId, Vec<(ExtendedNftTransferEvent, EventContext)>>,
    }

    #[async_trait]
    impl NftEventHandler for TestHandler {
        async fn handle_mint(&mut self, _mint: ExtendedNftMintEvent, _context: EventContext) {}

        async fn handle_transfer(
            &mut self,
            transfer: ExtendedNftTransferEvent,
            context: EventContext,
        ) {
            let entry = self
                .transfer_events
                .entry(context.tx_sender_id.clone())
                .or_insert_with(Vec::new);
            entry.push((transfer, context));
        }

        async fn handle_burn(&mut self, _burn: ExtendedNftBurnEvent, _context: EventContext) {}

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = NftIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 117_998_763,
                end_exclusive: Some(117_998_773),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
            .transfer_events
            .get(&"marketplace.paras.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            ExtendedNftTransferEvent {
                event: NftTransferEvent {
                    authorized_id: Some("marketplace.paras.near".parse().unwrap()),
                    old_owner_id:
                        "4eecd552b9774725d375337607cad4fe7afe99dd2fa9a6bcbb4c0d1b724c63f7"
                            .parse()
                            .unwrap(),
                    new_owner_id: "moehtetmyint.near".parse().unwrap(),
                    token_ids: vec!["501732:654".to_owned()],
                    memo: None,
                },
                trade: NftTradeDetails {
                    token_prices_near: vec![Some(790000000000000000000000)],
                }
            },
            EventContext {
                transaction_id: "5aPiGXDKi696Af6imrPMF3aQozQGZy119uM6WKRAqbVH"
                    .parse()
                    .unwrap(),
                receipt_id: "Cy8NNUDiDBmKyQ714CoyYV2MMzwxFuQoeVZnvDsCtdeJ"
                    .parse()
                    .unwrap(),
                block_height: 117998765,
                block_timestamp_nanosec: 1714543285352206574,
                tx_sender_id: "marketplace.paras.near".parse().unwrap(),
                contract_id: "x.paras.near".parse().unwrap(),
            }
        )]
    );
}

#[tokio::test]
async fn detects_mintbase_trade() {
    struct TestHandler {
        transfer_events: HashMap<AccountId, Vec<(ExtendedNftTransferEvent, EventContext)>>,
    }

    #[async_trait]
    impl NftEventHandler for TestHandler {
        async fn handle_mint(&mut self, _mint: ExtendedNftMintEvent, _context: EventContext) {}

        async fn handle_transfer(
            &mut self,
            transfer: ExtendedNftTransferEvent,
            context: EventContext,
        ) {
            let entry = self
                .transfer_events
                .entry(context.tx_sender_id.clone())
                .or_insert_with(Vec::new);
            entry.push((transfer, context));
        }

        async fn handle_burn(&mut self, _burn: ExtendedNftBurnEvent, _context: EventContext) {}

        async fn flush_events(&mut self, _block_height: BlockHeight) {}
    }

    let handler = TestHandler {
        transfer_events: HashMap::new(),
    };

    let mut indexer = NftIndexer(handler);

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(BlockRange::Range {
                start_inclusive: 116_934_524,
                end_exclusive: Some(116_934_529),
            })
        },
    )
    .await
    .unwrap();

    assert_eq!(
        *indexer
            .0
            .transfer_events
            .get(&"simple.market.mintbase1.near".parse::<AccountId>().unwrap())
            .unwrap(),
        vec![(
            ExtendedNftTransferEvent {
                event: NftTransferEvent {
                    authorized_id: Some("simple.market.mintbase1.near".parse().unwrap()),
                    old_owner_id: "beanlabs.near".parse().unwrap(),
                    new_owner_id:
                        "1e6b6b9181fe2a977eb67591322d3154ac6629617d5a00dfa13fc2367f4b9851"
                            .parse()
                            .unwrap(),
                    token_ids: vec!["348".to_owned()],
                    memo: None
                },
                trade: NftTradeDetails {
                    token_prices_near: vec![Some(2925000000000000000000000)]
                }
            },
            EventContext {
                transaction_id: "HLdiNk9QFS2AdRLNrWGfB6TzSHFRUy9TpmSjJK3escHa"
                    .parse()
                    .unwrap(),
                receipt_id: "Cvn41HotTFo7TkacdzPyKtzMhzbXRFY64kmK6zF9GzKx"
                    .parse()
                    .unwrap(),
                block_height: 116934526,
                block_timestamp_nanosec: 1713231344389999053,
                tx_sender_id: "simple.market.mintbase1.near".parse().unwrap(),
                contract_id: "beanlabs.mintbase1.near".parse().unwrap()
            }
        )]
    );
}
