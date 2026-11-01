#!/bin/bash
#
# Print the end of every node's log from the newest test directory, the run
# that failed when run after test/run_tests.sh fails. Each log is a
# collapsible group in a GitHub Actions log.
#
# usage: test/show_logs.sh [lines]

set -o errexit
set -o nounset

lines=${1:-60}

cd "$(dirname "$0")/.."

dir=
for d in test_run.*/; do
    if [ -d "$d" ] && [ "$d" -nt "$dir" ]; then
        dir=$d
    fi
done
if [ -z "$dir" ]; then
    echo "no test directory"
    exit 0
fi

for log in "$dir"logs/*.log; do
    [ -f "$log" ] || continue
    echo "::group::$log"
    tail -n "$lines" "$log"
    echo "::endgroup::"
done
