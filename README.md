# Nft Indexer

This indexer watches for NFT events (mint, transfer, burn) and sends them to Redis streams `nft_mint`, `nft_transfer`, and `nft_burn` respectively.

To run it, set `REDIS_URL` environment variable and `cargo run --release`
