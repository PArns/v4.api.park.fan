-- External validation of the harness: the 2026-10 review's own 15-min numbers,
-- recomputed from this run's per-origin aggregates (PAR-827 critic S15).
--
-- The review scored 33 sample parks over two target windows. Its figures are
-- unweighted slot MAEs over those parks' truth slots, which is exactly
-- sum(sae__<model>) / sum(n__<model>) over parts/slot restricted to the same
-- parks, target days and leads. Nothing here is reference-dependent, so this
-- query is unaffected by the reference-selection change.
--
-- Run (celestrial, read-only, 2 CPU / 2500 MB container). It writes
-- sanity_33parks.csv next to this file; copy it into both tables_*/ dirs:
--   docker run --rm --cpus 2 --memory 2500m --cpu-shares 256 --user 1000:1000 \
--     -e HOME=/tmp -v <results>:/app/results --entrypoint nice ml-bench:<tag> -n 10 \
--     python -c "import duckdb; c=duckdb.connect(); c.execute(\"SET memory_limit='1600MB'\"); \
--       c.execute(\"SET threads=2\"); \
--       c.execute(open('/app/results/20261009-baselines-v2/sanity_33parks.sql').read())"
-- The 33 park ids are the review's sample (scratchpad/parks_sample.txt), inlined
-- below so this file is self-contained.
COPY (
WITH s AS (
  SELECT * FROM read_parquet('/app/results/20261009-baselines-v2/parts/slot/*.parquet',
                             union_by_name => true)
),
w AS (
  SELECT 'review window A: 2026-08-15 .. 2026-09-10' AS window, DATE '2026-08-15' AS d0,
         DATE '2026-09-10' AS d1
  UNION ALL
  SELECT 'review window B: 2026-09-11 .. 2026-10-07', DATE '2026-09-11', DATE '2026-10-07'
),
p AS (SELECT unnest(['efddf830-194b-4959-a51a-0477ab51ebde','1d91d08f-a6b8-423c-a22a-e115e9a233d8','f0fe79fa-92c8-4ec4-a518-b8138c522d61','925b40a7-1bec-4f5e-b5ec-642d5bbdafda','8355a0b7-26e9-4a90-af47-246ec143e99a','9a906f3b-0bb2-45b6-b23c-879d0961f1a5','8c91d61b-811a-457f-803d-a02700b09a1b','17557c1d-6dbc-431a-b508-abdbd778b783','fa3619cc-6d95-425b-a37b-1f12dd54a792','aff7ff8f-a897-49d5-a65e-3932c2c79a5a','e119ba74-0e0e-49f2-bf33-d4db83722b32','f1dbc515-5861-408c-a2e6-99796d11a272','47782da5-f69b-4b2f-adc9-ec50bd955c2b','61aae01e-4f86-448c-8713-35b80abea1d5','3d8895ea-03ac-4634-b9ee-77c223858610','c859693b-662b-45c0-aed3-29b7b719939e','204177ab-044e-484b-bdf4-794b8c49c312','60695a74-612f-4b09-b053-4768f1fe1910','1e1bfe68-87ef-44d4-ac65-70ce45542ea2','9b8209b9-cde2-42f6-8842-71ebcf848e5b','d959cb23-52e2-46ba-b752-31b681cf3bcf','313ca334-c04f-4855-bfd9-c8beb7a00d98','ef00a632-3cb6-482f-8d0f-ac29575d78ef','6ba1074b-68b6-4646-bbf7-58f3be700444','843c4acf-bd24-4897-88b1-1cd6e787aa9f','d0397b79-efc1-413b-8c6b-16e7bcf3fbd1','9158aa77-ee0b-4e4e-b656-ef513623026f','9a0d044a-7637-44f2-86b3-64f2bb9c2c0e','3c9397df-a433-4abe-ad88-f2ac27c7f2d9','a1594244-0325-46fa-b0ce-2a9ab106f433','bdeed40e-36ac-4e84-bfb1-ed44ae2e9dee','5c9f9cbf-9277-4a42-b70d-ec30445ba15f','3b5f11bd-cf4d-4f36-93d1-ff334cefde3b']) AS park_id),
scoped AS (
  SELECT w.window, s.L AS lead,
         CASE WHEN s.park_id IN (SELECT park_id FROM p) THEN '33 review parks' ELSE 'other parks' END
           AS park_set,
         s.* EXCLUDE (L, park_id, date)
  FROM s JOIN w ON s.date BETWEEN w.d0 AND w.d1
)
SELECT window, park_set, lead,
       sum(n_truth)                                 AS truth_slots,
       sum(sae__wt_med)     / sum(n__wt_med)        AS wt_med,
       sum(sae__clim)       / sum(n__clim)          AS clim,
       sum(sae__h5)         / sum(n__h5)            AS h5,
       sum(sae__lvlh5_naive)/ sum(n__lvlh5_naive)   AS lvlh5_naive,
       sum(sae__lvlh5_tft)  / sum(n__lvlh5_tft)     AS lvlh5_tft
FROM scoped WHERE lead IN (1, 7) GROUP BY ALL ORDER BY window, park_set DESC, lead
) TO '/app/results/20261009-baselines-v2/sanity_33parks.csv' (FORMAT csv, HEADER);
