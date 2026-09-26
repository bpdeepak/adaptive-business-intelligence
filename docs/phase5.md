# Phase 5 — Close the loops, deploy for free, pricing + seller-funnel attribution

**Status: Stage 5A (close the loops) complete and verified; 5B (observability + free
deploy) next.** The plan and its
decisions are in the approved Phase 5 plan: sequence 5A → 5B → 5C → 5D, zero-cost hosting
(Oracle Always Free), Groq's free tier for the agent, seller-funnel analytics labeled
honestly, and advisory-only pricing. This document follows the phase-doc format: every
claim cites the test that pins it.

## 5A.1 Churn scoring: the retention playbooks get a producer

| Claim | Evidence |
|---|---|
| Training and scoring compute churn features with ONE definition: a dbt macro shared by `feature_customer_churn` and the new `feature_customer_churn_current` | `dbt/macros/churn_features.sql`; `ml/tests/test_churn_scoring_mart.py`; the training mart was byte-identical before and after the extraction (2,892 rows, same md5) |
| The scoring population is exactly the right-censored repeat customers: latest order in the final 90 days, so the outcome is not yet observable. As-of = the latest order, and nothing uses `now()` | `feature_customer_churn_current.sql`; `dbt/tests/singular/churn_current_disjoint_from_training.sql` (338 customers) |
| The scorer sends exactly the features the active registry version was trained on | `TestChurnScorerScoresThePopulationOncePerVersionAndResumes` (the fake sidecar refuses any other vector, like the real one) |
| It scores once per active model version and resumes an interrupted pass; it never re-scores on a timer | same test (second pass = 0, resume = exactly the missing rows) |
| A missing registry threshold fails closed (never 0) | `TestActiveModelFailsClosedWithoutAThreshold` |
| Retention offers go to the top 20 by risk only (`score >= 0.70 AND rank <= 20`); the rank has one SQL definition shared by the scorer and the reconciler | `TestShippedRetentionRuleProposesExactlyTheTop20`; `TestReconcileRecoversChurnProposals` (rank survives reconciliation) |
| A dropped `churn_scored` event is rebuilt from the persisted row | `TestReconcileRecoversChurnProposals` |

Policy note: against the real model, 116 of 338 customers clear 0.70, and the model is
likely miscalibrated for the 2018 regime (it trained at a 37.9 % base churn rate; the
late-2018 rate is about 10 %). The rank cap makes a reviewer work down from the top, as
the Phase 2 card recommends. The auto-tier nudge (`score < 0.30`, informational log
only) covers about 103 customers per model version.

## 5A.2 Demand forecast worker, and the forecast inputs corrected

| Claim | Evidence |
|---|---|
| The forecast features are one macro shared by the training mart and the next-week scoring mart; every feature is a lag or trailing window | `dbt/macros/forecast_features.sql`; `ml/tests/test_forecast_marts.py` |
| The same-week `avg_order_value` leak is gone (`aov_lag1` replaces it) | `test_trainer_has_no_same_week_aov_feature`; `dbt/tests/singular/forecast_features_have_no_same_week_aov.sql` |
| The extraction-cutoff tail is excluded; the scoring week is exactly the week after the last complete week | `dbt/tests/singular/forecast_next_week_is_one_week_after_training.sql` |
| The worker forecasts next-week ORDERS for every category the model trained on, skipping (and counting) categories outside the training rank, once per active version | `TestForecastWorkerForecastsEveryTrainedCategoryOncePerVersion` |
| `forecast_updated` is built by one constructor, pinned to the declared playbook fields with no exemption left | `events/payloads.go` `NewForecastUpdated`; `TestDeclaredFieldsMatchWhatProducersEmit` |
| The shipped rule `draft-po-on-demand-surge` proposes only for a genuine surge (> 1.2× AND ≥ 5 orders over the trailing 4-week mean); approval drafts a PO for `ceil(forecast − mean)` | `TestShippedDemandSurgeRuleDraftsAPurchaseOrderForTheExcess` |
| A dropped `forecast_updated` is rebuilt from the persisted row | `TestReconcileRecoversForecastProposals` |

The Phase 4 dormant rule (`draft-po-on-weak-forecast`, `point_estimate < recent_avg × 1.1`)
proposed a purchase order when demand was **weak**. It would have fired for 41 of 67
categories. The replacement fires for 2 on the corrected model: `computers_accessories`
(forecast 157.9 vs a 116.5 trailing mean) and `food` (28.6 vs 23.5). Verified on dev
through the real sidecar code (`ml/serve.py` `ModelStore.score`): 67 categories served,
0 refused, 5 skipped as outside the training rank. Olist has no inventory data, so a PO here is demand
cover, not a stock calculation, and its reference says so.

## Batch delivery: waiting publish

The in-process bus drops events for a slow subscriber by design, so the scoring hot
path never blocks. A batch producer's burst is different: the churn scorer's 338 events
lost 273 on the plain `Publish`. The reconciler rebuilt them all, but most batch
proposals then arrived a tick late, marked `reconciled`. Batch producers now use
`Bus.PublishWait`, which waits up to 5 s per subscriber and only then counts a drop.
The same burst drops 0. Tests: `TestPublishWaitDeliversABurstWithoutDropping`,
`TestPublishWaitCountsADropWhenASubscriberIsStuck`; the end-to-end tests log the drop
count.

## 5A.4 `gold.predictions` retention (the phase4 §11.21 policy, built)

| Claim | Evidence |
|---|---|
| Aged `stream_score` (30 d) and `live_score` (90 d) rows MOVE to `gold.predictions_archive`; nothing is deleted | `TestRetentionArchivesOnlyWhatThePolicyAllows` (archived rows keep their metadata) |
| Held-out/backtest evidence and the batch sources (`churn_score`, `forecast_score`) are never archived | same test |
| Nothing above the governance reconciler's cursor moves; with no cursor at all, nothing moves | same test; `TestRetentionMovesNothingWithoutAReconcileCursor` |
| A row cited by a pending or failed action stays hot; one cited by a decided action may go | same test |
| Each chunk (≤ 1,000 rows) is one atomic delete-returning → insert statement; chunking does not change the outcome; runs are idempotent | same test (forced to 1-row chunks; second run leaves nothing eligible) |
| Every run writes one `gold.event_log` accounting row (`kind='retention'`: moved count, id range, windows) | same test |
| The archive carries every `gold.predictions` column | `TestArchiveColumnsMatchThePredictionsTable`; `test_serving_schema_declares_the_predictions_archive` |

Age is measured on `created_at` (persistence time), never on event time. Windows and
cadence are configured by `ABI_RETENTION_STREAM_SCORES` (720h), `ABI_RETENTION_LIVE_SCORES`
(2160h) and `ABI_RETENTION_PREDICTIONS_EVERY` (6h). The agent's `model_scores` /
`model_rate` descriptions now say they read the hot table only.

## 5A.5 Deployable images and the production compose file

| Claim | Evidence |
|---|---|
| Go image: one Dockerfile builds `server` or `producer` (distroless, nonroot; 39 MB / 27 MB); the server image carries `config/playbooks.yml`; the stale `api/Dockerfile` (it built the retired `cmd/api`) is gone | `deploy/docker/go.Dockerfile` |
| Python image: ONE image for the model sidecar, agent, drift monitor, retrain worker and bootstrap, from the frozen `uv.lock` (ml group included); 2.26 GB on x86, down from 6.1 GB once a recursive `chown` layer and the uv cache stopped duplicating the virtualenv (≈290 MB less on arm64: no NVIDIA NCCL wheel) | `deploy/docker/python.Dockerfile` |
| `deploy/compose.prod.yml` runs the whole stack on a private network: Redpanda advertises `redpanda:9092` (the dev compose advertises `localhost`, which only works from the host); only the server's HTTP port is published, on loopback | validated with `docker compose config`; run end to end below |
| A fresh machine bootstraps from nothing with one command | `deploy/bootstrap.sh` via the `bootstrap` profile: download, bronze, dbt (PASS=129), feature stores, all four model families trained and registered, manifest; exit 0 on an empty database |
| The batch producers do not race the model sidecar at boot | sidecar healthcheck + `depends_on: service_healthy`; failed passes retry within a minute (`TestPollLoopRetriesAFailedPassSoon`, `TestPollLoopWaitsTheFullIntervalAfterSuccess`) |
| End to end on the fresh stack | API numbers correct (revenue 15,739,137.01, 98,206 orders, AOV 160.27); live replay buckets; sidecar and agent up; about 11k stream scores; churn 338 scored → **20** retention proposals + 100 auto nudge logs; forecast 67 categories (5 untrained skipped) → **2** PO proposals; retention job runs logged |

Found while building the images (both fixed with tests):

* **Artifact paths were Windows paths.** Trainers stored `str(path)`, so the registry
  and manifest held `artifacts\fraud_risk\….joblib`, which a Linux container cannot
  open. `common.portable_path` stores POSIX paths (`register_model` and the manifest),
  and the sidecar normalises rows registered earlier. Test:
  `ml/tests/test_artifact_paths.py`. The loader test only proves the bug on Linux (CI);
  Windows accepts both separators.
* **Training depended on the database's physical row order.** The same data in a
  freshly loaded database gave a different model. The trainers' queries had no
  `ORDER BY`, the time split is stable (ties keep input order), and XGBoost's row
  subsampling follows row order. Every trainer now orders its frame by its key
  (`common.canonical_order`), and `category_rank_from` breaks ties by name. Test:
  `ml/tests/test_training_reproducibility.py` (identical split and predictions for two
  row orders; fails before the fix).
* **The Windows XGBoost build is not deterministic across thread counts; Linux is.**
  The remaining dev-vs-fresh difference was the platform, not the data: trained inside
  the Linux container, the dev database reproduces the fresh build exactly. The canonical
  models are therefore the ones the VM trains on Linux. The churn `recommended_threshold`
  is fragile (0.595 ↔ 0.925 at the same AUC on such perturbations; about 56 churned
  customers in the test split), which is why the retention rule uses a score floor plus a
  rank cap rather than the registry threshold. The fraud threshold (0.835) is stable
  across platforms.

## Dev retrain (done, 2026-09-26)
The dev database was rebuilt with `dbt build` (PASS=129; the new marts
`feature_customer_churn_current` has 338 rows, `feature_forecast_next_week` 72) and
both forecast models were retrained. Version **`20260926.093742`** is active, and
`20260924.050026` is superseded. It reproduces the clone exactly: WMAPE revenue 0.3357,
orders 0.2811. The serving manifest was regenerated. The model sidecar must be
restarted to serve it.
