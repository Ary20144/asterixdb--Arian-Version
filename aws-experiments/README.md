# NLJ static-memory experiments — AWS runbook

Everything needed to run the customer ⋈ store_returns nested-loop-join ladder on a
fresh machine. **No debugging on AWS**: every script here must be dry-run against a
local cluster first; AWS is execution only.

## What this branch contains
`master-static-logging` = stock AsterixDB master + three things:
1. `NestedLoopJoin.java`: counters/log lines only (`NLJ-STATIC spill N` per buffer-full
   pass, `NLJ-STATIC SUMMARY` at completion). No behavior change.
2. `JoinUtils.java`: forces the nested-loop join path (master would pick hybrid hash
   join for this equijoin).
3. 1-NC / 1-iodevice test configs.

## Machine
- EC2 with >= 16 GB RAM and >= 100 GB gp3 (e.g. r6i.xlarge). Single node.
- Install: JDK 17, Maven 3.9+, git, jq.

## One-time setup
```bash
git clone -b master-static-logging https://github.com/Ary20144/asterixdb--Arian-Version.git asterixdb
cd asterixdb/asterixdb && mvn clean package -DskipTests          # ~20-40 min
# unpacked server appears under asterix-server/target/asterix-server-*-binary-assembly/
```
Start the sample cluster from the unpacked server dir (`opt/local/bin/start-sample-cluster.sh`).
Before starting, check `opt/local/conf/cc.conf`:
- exactly one NC (`asterix_nc1`) with exactly one iodevice;
- NC JVM heap at least 2x the largest join budget (>= 2 GB is plenty: `-Xmx2g`).

Upload the data files (scp) to the path in `config.sh` (`DATA_DIR`):
`customer.json`, `store_returns.json`.

Edit `config.sh`: `DATA_DIR`, `NC_LOG` (point at the real NC log file).

## Run order
```bash
bin/setup_data.sh     # create datasets (compression off) + load
bin/verify.sh         # customer=100000, oracle join count=277498  -- STOP if wrong
bin/run_ladder.sh     # 5 rungs x 4 runs; ~20-25 min per run locally
```
Outputs land in `results/`: one JSON per run (metrics + per-operator profile
timings + annotated plan) and `summaries.log` (the NLJ-STATIC lines per run).

## The ladder (build = 2,005 frames x 32 KB = 65,699,840 B)
budget = (ceil(p% x 2005) + 2 reserved frames) x 32 KB

| rung | joinmemory | passes | spills | sFrameReads |
|------|-----------|--------|--------|-------------|
| 10%  | 6496KB    | 10     | 9      | 59250       |
| 20%  | 12896KB   | 5      | 4      | 29625       |
| 40%  | 25728KB   | 3      | 2      | 17775       |
| 80%  | 51392KB   | 2      | 1      | 11850       |
| 100% | 64224KB   | 1      | 0      | 5925        |

Every run must show `matches=277498 outerFramesIn=2005 outerBytesIn=65699840
innerRunFileBytes=194150400` and the rung's `outerBudgetFrames`. A run that
misses any of these is invalid: discard and rerun, do not average it in.

Report per rung: mean and SD of `metrics.executionTime` over runs 2-4 (run 1 is
the warm-up and is discarded).

## Profiling a single run by hand
```bash
jq -n --arg stmt 'USE tpcds; SET `compiler.joinmemory` "6496KB"; SELECT * FROM customer c, store_returns sr WHERE sr.sr_customer_sk = c.c_customer_sk LIMIT 1;' \
  '{statement:$stmt, profile:"timings", "optimized-logical-plan":true, "plan-format":"json"}' \
  | curl -s -X POST http://localhost:19002/query/service -d @- -H 'Content-Type: application/json' > run.json
jq '.metrics' run.json                 # executionTime / elapsedTime
jq '.profile' run.json                 # per-operator time breakdown
jq '.plans' run.json                   # optimized plan, annotated
```
