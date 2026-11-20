#!/usr/bin/env bash
cd /opt/dw-benchmarks || exit 1
r() { echo "rest: start $*"; ./bench run "$@" || echo "rest: FAILED $*"; }
./bench down synapse --disk crypt || true
for s in latency-ladder fanout; do
  r dotwire-sse "$s" --reps 1
  for v in presend postsend ratelimits; do
    r dotwire "$s" --variant "$v" --reps 1
  done
done
for s in latency-ladder history; do
  r tinode "$s" --disk plain
done
export CONFIG=harness/configs/synapse.toml
for s in latency-ladder fanout connections reconnect-storm history; do
  r synapse "$s" --disk crypt
done
for s in latency-ladder history; do
  r synapse "$s" --disk plain
done
echo "rest: done"
