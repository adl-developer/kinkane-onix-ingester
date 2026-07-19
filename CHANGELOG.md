# 1.0.0 (2026-07-19)


### Bug Fixes

* commit missing Gardners npm dependencies to package.json ([f749ebf](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/f749ebf1b8fe89c4e05acd51f92c50fc7fa812b1))
* detect SSL requirement from the connection string, not NODE_ENV ([06d4881](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/06d4881cdf1220255e7d61fe91c1157aa560e85a))
* pin embedding output dimension and run backfill in background ([0c9b73b](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/0c9b73b3b1c3a883a2471cf87846bdb71466595f))
* raise DB connection pool size to cover real worker concurrency ([4cd2119](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/4cd2119af53cb19bc483a4716f75f9f61f9a284b))
* recover if bootstrap crashes between landing biblio and ingesting it ([8f38834](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/8f38834e9aeccfeef44c06bb98ffd9144d7b0024))
* soft-delete withdrawn books instead of destroying user content ([19ec77f](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/19ec77fd03d97c691e41c46b7417a1be201251a9))
* stop a DB failure after R2 delete from permanently poisoning a chunk ([3e50622](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/3e506225d0a24edc6305bdbc855007d31f9d2f61))
* stop concurrent FTP cover downloads from crashing the client ([42a30fa](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/42a30fac09f830cb014a0133a01d5f2331127852))
* stop duplicate re-ingestion of already-processed ONIX files ([a1a1686](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/a1a16869bcad7b30eb2307395f72e21861db106b))
* wire up Gardners config, schema barrel, and R2 stream upload ([f7ee536](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/f7ee5363c17d389e46408802519f1bca7e4aca88))


### Features

* add admin endpoint to bootstrap the full Gardners catalogue ([8e763df](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/8e763dfc699a1d2e9e1915a6fcaea5ada06cc45f))
* add Avail13 hourly stock feed (Feed 3) ([7d3fc42](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/7d3fc4217c1af9c1571648e6b9d1801ec2f66221))
* add Bespoke Inventory feed ingestion (CSV chunk-queue pattern) ([8fbea65](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/8fbea655bdb24cc163513a05200cb0298ce52b5a))
* add DB schema and SFTP/FTP fetcher framework for Gardners feeds ([3356d09](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/3356d090a9c1fac2bb5a77a61d5ab05c68b8368d))
* add Gardners cover image sync (Step 7, final feed) ([5cf76d4](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/5cf76d4511f132da9ea74e9cf78bbd7fe22d2d29))
* add GARDNERS_INGESTION_ENABLED master switch for all Gardners work ([4da2a0e](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/4da2a0e6e2f65f9180b2c32390309c8710041af0))
* add ONIX Biblio feed ingestion + fix SFTP throughput and retry bugs ([5ebb1f8](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/5ebb1f8d4f31d9d9ac08f6cc454ceecc5df17621))
* add POST /api/ingestion/backfill-embeddings admin endpoint ([1f1b831](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/1f1b8311e048461827fe4a4891c59e0556910f47))
* add Promotions, isbn-slips, Firm Sale, and mkres feeds (Step 5) ([fe9496f](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/fe9496f0c61520dd1977ecd3de29adb022022d3d))
* backfill bookId on gardners_stock rows after each chunk upsert ([50f7d4b](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/50f7d4be765ac24789d3311cc5247f3ebc0f4e9d))
* extend admin token expiry from 30m to 72h ([98380cf](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/98380cf6435128659b0792a009321bbcc2d583de))
* make Google Books cover fallback a true last resort ([b355637](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/b355637bb381f5bffc47790ab26a39e57c10511b))
* move chunk payloads from PostgreSQL JSONB to R2 object storage ([bcf2404](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/bcf240446d3f6b12fd6d8615110ca147a168afb0))
* run the cover backfill over multiple concurrent FTP connections ([d179c9f](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/d179c9f2d1365c48125eb7a075c7cafa3a6158f6))


### Reverts

* put admin token expiry back to 30m ([43c13db](https://adl.github.com/adl-developer/kinkane-onix-ingester/commit/43c13dba86616857477dce560ee5b465ce809a7e))
