#!/usr/bin/env bash
# Runs a RaftKV cluster of N nodes (default 3) on this machine, one process per node.
#
#   scripts/run_cluster.sh start [N]     start N nodes in the background
#   scripts/run_cluster.sh stop [ID]     stop one node (graceful), or all
#   scripts/run_cluster.sh kill ID       crash a node (SIGKILL): it gets no chance to clean up
#   scripts/run_cluster.sh restart ID    stop (if running) and start one node again
#   scripts/run_cluster.sh status        role, term and log positions of every node
#   scripts/run_cluster.sh leader        print the id of the current leader
#   scripts/run_cluster.sh cli ARGS...   run raftkv_cli against this cluster (put/get/delete/bench/repl)
#   scripts/run_cluster.sh logs ID       follow one node's log
#   scripts/run_cluster.sh clean         stop everything and delete all data
#
# Environment (all optional):
#   RAFTKV_BIN              directory with raftkv_node and raftkv_cli (default: build-net/bin)
#   RAFTKV_DIR              data and logs (default: /tmp/raftkv-cluster)
#   RAFTKV_RAFT_PORT_BASE   node i listens for other nodes on base+i (default 7000)
#   RAFTKV_CLIENT_PORT_BASE node i listens for clients on base+i (default 7100)
#   RAFTKV_NODE_FLAGS       extra raftkv_node flags, e.g. "--no-sync" for a fast, non-durable demo
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${RAFTKV_BIN:-$ROOT/build-net/bin}"
DIR="${RAFTKV_DIR:-/tmp/raftkv-cluster}"
RAFT_BASE="${RAFTKV_RAFT_PORT_BASE:-7000}"
CLIENT_BASE="${RAFTKV_CLIENT_PORT_BASE:-7100}"
NODE_FLAGS="${RAFTKV_NODE_FLAGS:-}"

die() { echo "run_cluster.sh: $*" >&2; exit 1; }

need_binaries() {
  [ -x "$BIN/raftkv_node" ] && [ -x "$BIN/raftkv_cli" ] ||
    die "cannot find raftkv_node and raftkv_cli in $BIN (build with -DRAFTKV_BUILD_NETWORK=ON, or set RAFTKV_BIN)"
}

node_count() {
  if [ -f "$DIR/nodes" ]; then cat "$DIR/nodes"; else echo 3; fi
}

peers_list() {
  local n="$1" i out=""
  for i in $(seq 1 "$n"); do out="$out${out:+,}$i=127.0.0.1:$((RAFT_BASE + i))"; done
  echo "$out"
}

servers_list() {
  local n="$1" i out=""
  for i in $(seq 1 "$n"); do out="$out${out:+,}$i=127.0.0.1:$((CLIENT_BASE + i))"; done
  echo "$out"
}

is_running() {
  local pidfile="$DIR/node$1.pid"
  [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null
}

start_node() {
  local id="$1" n
  n="$(node_count)"
  if is_running "$id"; then echo "node $id is already running"; return 0; fi
  mkdir -p "$DIR/node$id"
  : >> "$DIR/node$id.log"
  local before
  before="$(wc -l < "$DIR/node$id.log")"
  # shellcheck disable=SC2086
  nohup "$BIN/raftkv_node" --id "$id" --peers "$(peers_list "$n")" \
    --client-addr "127.0.0.1:$((CLIENT_BASE + id))" --data-dir "$DIR/node$id" $NODE_FLAGS \
    >> "$DIR/node$id.log" 2>&1 &
  echo $! > "$DIR/node$id.pid"
  local waited=0
  while [ "$waited" -lt 100 ]; do  # up to 10 s
    if ! is_running "$id"; then
      echo "node $id exited during startup; see $DIR/node$id.log:" >&2
      tail -n 5 "$DIR/node$id.log" >&2
      return 1
    fi
    if tail -n +"$((before + 1))" "$DIR/node$id.log" | grep -q "READY"; then
      echo "node $id started (pid $(cat "$DIR/node$id.pid"), client port $((CLIENT_BASE + id)))"
      return 0
    fi
    sleep 0.1
    waited=$((waited + 1))
  done
  die "node $id did not become ready within 10 seconds; see $DIR/node$id.log"
}

stop_node() {  # graceful, then forceful
  local id="$1"
  if ! is_running "$id"; then echo "node $id is not running"; rm -f "$DIR/node$id.pid"; return 0; fi
  local pid
  pid="$(cat "$DIR/node$id.pid")"
  kill -TERM "$pid" 2>/dev/null
  local waited=0
  while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 50 ]; do sleep 0.1; waited=$((waited + 1)); done
  if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null; echo "node $id did not stop in time: killed"; else echo "node $id stopped"; fi
  rm -f "$DIR/node$id.pid"
}

crash_node() {
  local id="$1"
  if ! is_running "$id"; then echo "node $id is not running"; return 0; fi
  kill -KILL "$(cat "$DIR/node$id.pid")" 2>/dev/null
  rm -f "$DIR/node$id.pid"
  echo "node $id crashed (SIGKILL)"
}

cli() {
  need_binaries
  "$BIN/raftkv_cli" --servers "$(servers_list "$(node_count)")" "$@"
}

cmd="${1:-}"
[ -n "$cmd" ] && shift
case "$cmd" in
  start)
    need_binaries
    n="${1:-3}"
    case "$n" in ''|*[!0-9]*) die "N must be a number";; esac
    [ "$n" -ge 1 ] || die "N must be at least 1"
    mkdir -p "$DIR"
    echo "$n" > "$DIR/nodes"
    for i in $(seq 1 "$n"); do start_node "$i" || exit 1; done
    echo "cluster of $n nodes is up. Try:  $0 cli put hello world   then   $0 status"
    ;;
  stop)
    if [ $# -ge 1 ]; then stop_node "$1"; else for i in $(seq 1 "$(node_count)"); do stop_node "$i"; done; fi
    ;;
  kill)
    [ $# -eq 1 ] || die "usage: kill ID"
    crash_node "$1"
    ;;
  restart)
    [ $# -eq 1 ] || die "usage: restart ID"
    need_binaries
    is_running "$1" && stop_node "$1"
    start_node "$1"
    ;;
  status)
    cli status
    ;;
  leader)
    cli status | awk '$3 == "leader" { print $2; found = 1 } END { exit found ? 0 : 1 }'
    ;;
  cli)
    cli "$@"
    ;;
  logs)
    [ $# -eq 1 ] || die "usage: logs ID"
    tail -n 50 -f "$DIR/node$1.log"
    ;;
  clean)
    for i in $(seq 1 "$(node_count)"); do is_running "$i" && stop_node "$i" > /dev/null; done
    rm -rf "$DIR"
    echo "removed $DIR"
    ;;
  *)
    sed -n '2,/^set -u/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
    [ -z "$cmd" ] || exit 2
    ;;
esac
