#!/usr/bin/env bash
set -euo pipefail

out="${1:?usage: sampler.sh <out.jsonl>}"
trap 'exit 0' TERM INT

now_ms() {
  local ms
  ms=$(date +%s%3N)
  case "$ms" in
    *N*) ms=$(($(date +%s) * 1000)) ;;
  esac
  echo "$ms"
}

host_cpu() {
  awk '/^cpu /{printf "[%s,%s,%s,%s,%s,%s,%s,%s]", $2,$3,$4,$5,$6,$7,$8,$9}' /proc/stat
}

host_net() {
  awk 'BEGIN{printf "{"}
    NR>2 {sub(/^ +/, ""); split($0, f, /[: ]+/); if (f[1] != "lo") {printf "%s\"%s\":[%s,%s]", s, f[1], f[2], f[10]; s=","}}
    END{printf "}"}' /proc/net/dev
}

host_disk() {
  awk '$3 ~ /^(nvme[0-9]+n[0-9]+|sd[a-z]+|dm-[0-9]+)$/ {printf "%s\"%s\":[%s,%s,%s,%s]", s, $3, $4, $6, $8, $10; s=","}
    BEGIN{printf "{"} END{printf "}"}' /proc/diskstats
}

host_json() {
  if [ -r /proc/stat ]; then
    printf '{"cpu":%s,"net":%s,"disk":%s}' "$(host_cpu)" "$(host_net)" "$(host_disk)"
  else
    printf '{}'
  fi
}

docker_json() {
  local out
  out=$(docker stats --no-stream --format '{{json .}}' 2>/dev/null | jq -sc '.') || out='[]'
  echo "$out"
}

while true; do
  printf '{"t":%s,"docker":%s,"host":%s}\n' "$(now_ms)" "$(docker_json)" "$(host_json)" >>"$out"
  sleep 1 &
  wait $! || true
done
