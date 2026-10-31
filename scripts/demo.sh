#!/usr/bin/env bash
# A guided demonstration of RaftKV on this machine: a 3-node cluster survives its leader crashing,
# changes membership while serving, answers a range scan, and at the end every acknowledged write
# is checked to still be there with its exact value.
#
#   scripts/demo.sh            run the whole demo (about a minute), then clean up
#   scripts/demo.sh --pause    wait for Enter between steps (for presenting)
#   scripts/demo.sh --keep     leave the cluster running afterwards (scripts/run_cluster.sh works on it)
#
# It uses its own data directory (RAFTKV_DIR, default /tmp/raftkv-demo), so it never touches a
# cluster started with run_cluster.sh's default directory. Writes are durable (fsync on), like a
# real deployment. RAFTKV_BIN works as for run_cluster.sh.
set -euo pipefail
cd "$(dirname "$0")/.."

PAUSE=0
KEEP=0
for arg in "$@"; do
  case "$arg" in
    --pause) PAUSE=1 ;;
    --keep) KEEP=1 ;;
    *) echo "usage: scripts/demo.sh [--pause] [--keep]" >&2; exit 2 ;;
  esac
done

export RAFTKV_DIR="${RAFTKV_DIR:-/tmp/raftkv-demo}"
RC=scripts/run_cluster.sh
ACKED="$(mktemp)"   # "key<TAB>value" of every write the cluster acknowledged
UNKNOWN="$(mktemp)" # keys whose write had an unknown outcome (it may or may not have happened)

cleanup() {
  rm -f "$ACKED" "$UNKNOWN"
  if [ "$KEEP" = 0 ]; then "$RC" clean > /dev/null 2>&1 || true; fi
}
trap cleanup EXIT

step() {
  printf '\n\033[1m== %s\033[0m\n' "$1"
  if [ "$PAUSE" = 1 ]; then read -r -p "   (Enter to continue) " _; fi
}
show() { printf '\033[2m$ %s\033[0m\n' "$*"; "$@"; }
fail() { printf '\n\033[31mDEMO FAILED: %s\033[0m\n' "$*" >&2; exit 1; }
now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'; }

leader() { "$RC" leader 2> /dev/null || true; }
wait_for_leader() {  # prints the leader's id once there is one
  local l=""
  for _ in $(seq 1 100); do
    l="$(leader)"
    case "$l" in '' | *[!0-9]*) sleep 0.1 ;; *) echo "$l"; return 0 ;; esac
  done
  fail "no leader was elected within 10 s"
}

# put KEY: writes value "v-KEY" and records whether the cluster acknowledged it.
put() {
  if "$RC" cli put "$1" "v-$1" > /dev/null 2>&1; then
    printf '%s\tv-%s\n' "$1" "$1" >> "$ACKED"
  else
    echo "$1" >> "$UNKNOWN"
  fi
}

step "1. Start a fresh 3-node cluster (data in $RAFTKV_DIR)"
"$RC" clean > /dev/null 2>&1 || true
show "$RC" start 3
L="$(wait_for_leader)"
echo "   node $L was elected leader"
show "$RC" status

step "2. Write 20 keys (each acknowledged write is durable on a majority)"
for i in $(seq -w 1 20); do put "key$i"; done
echo "   acknowledged: $(wc -l < "$ACKED" | tr -d ' ') of 20"
show "$RC" cli get key07

L="$(wait_for_leader)"  # ask again: leadership may have moved since step 1
step "3. Crash the leader (node $L, SIGKILL: no chance to clean up) and keep writing"
show "$RC" kill "$L"
start="$(now_ms)"
put key21  # waits while the remaining two nodes elect a new leader, then succeeds
took=$(( $(now_ms) - start ))
for i in $(seq 22 40); do put "key$i"; done
NEW="$(wait_for_leader)"
[ "$NEW" != "$L" ] || fail "node $L was crashed but is reported as leader"
echo "   node $NEW is the new leader; the first write after the crash took ${took} ms"
echo "   (that includes noticing the crash and electing node $NEW: followers wait 150-300 ms first)"
echo "   acknowledged so far: $(wc -l < "$ACKED" | tr -d ' ') of 40"
show "$RC" status

step "4. Bring the crashed node back: it rejoins as a follower and catches up from its own disk and the leader"
show "$RC" restart "$L"
sleep 2
show "$RC" status

step "5. Change membership while the cluster serves: add node 4, then remove a follower"
show "$RC" add 4
NEW="$(wait_for_leader)"
VICTIM=""
for id in 1 2 3; do
  if [ "$id" != "$NEW" ]; then VICTIM="$id"; break; fi
done
show "$RC" remove "$VICTIM"
for i in $(seq 41 50); do put "key$i"; done
show "$RC" status

step "6. A range scan (linearizable, like get): the keys from key10 up to, not including, key15"
show "$RC" cli scan key10 key15

step "7. Check: every acknowledged write is still there, with its exact value"
SCAN="$(mktemp)"
"$RC" cli scan key key~ 1000 > "$SCAN" || { rm -f "$SCAN"; fail "the final scan failed"; }
missing="$(sort "$ACKED" | comm -23 - <(sort "$SCAN") | wc -l | tr -d ' ')"
acked="$(wc -l < "$ACKED" | tr -d ' ')"
present="$(wc -l < "$SCAN" | tr -d ' ')"
unknown="$(wc -l < "$UNKNOWN" | tr -d ' ')"
rm -f "$SCAN"
echo "   acknowledged writes: $acked; keys in the store: $present; writes with an unknown outcome: $unknown"
if [ "$missing" != 0 ]; then
  fail "$missing acknowledged write(s) missing or changed"
fi
printf '   \033[32mall %s acknowledged writes are present with their exact values\033[0m\n' "$acked"
if [ "$unknown" != 0 ]; then
  echo "   (a write with an unknown outcome may or may not have taken effect; it is never counted as lost)"
fi

if [ "$KEEP" = 1 ]; then
  echo
  echo "The cluster is still running (data in $RAFTKV_DIR). Try:"
  echo "  RAFTKV_DIR=$RAFTKV_DIR $RC status"
  echo "  RAFTKV_DIR=$RAFTKV_DIR $RC cli scan - - 10"
  echo "  RAFTKV_DIR=$RAFTKV_DIR $RC clean      # when done"
fi
