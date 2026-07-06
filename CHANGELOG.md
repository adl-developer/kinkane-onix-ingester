# 1.0.0 (2026-07-06)


### Bug Fixes

* commit missing Gardners npm dependencies to package.json ([f749ebf](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/f749ebf1b8fe89c4e05acd51f92c50fc7fa812b1))
* pin embedding output dimension and run backfill in background ([0c9b73b](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/0c9b73b3b1c3a883a2471cf87846bdb71466595f))
* wire up Gardners config, schema barrel, and R2 stream upload ([f7ee536](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/f7ee5363c17d389e46408802519f1bca7e4aca88))


### Features

* add Bespoke Inventory feed ingestion (CSV chunk-queue pattern) ([8fbea65](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/8fbea655bdb24cc163513a05200cb0298ce52b5a))
* add DB schema and SFTP/FTP fetcher framework for Gardners feeds ([3356d09](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/3356d090a9c1fac2bb5a77a61d5ab05c68b8368d))
* add ONIX Biblio feed ingestion + fix SFTP throughput and retry bugs ([5ebb1f8](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/5ebb1f8d4f31d9d9ac08f6cc454ceecc5df17621))
* add POST /api/ingestion/backfill-embeddings admin endpoint ([1f1b831](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/1f1b8311e048461827fe4a4891c59e0556910f47))
* backfill bookId on gardners_stock rows after each chunk upsert ([996e279](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/996e279d49391f906d41b7855b2a6299ff934d0c))
* move chunk payloads from PostgreSQL JSONB to R2 object storage ([bcf2404](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/bcf240446d3f6b12fd6d8615110ca147a168afb0))
