#!/bin/bash
#
# Build Dynomite and run the functional tests against generated clusters.
#
# usage: test/run_tests.sh [-n|--no-rebuild] [test ...]
#
# A test is a request file, describing a cluster to run the functional
# tests against, plaintext and with mutual TLS between its nodes, or a test
# script that sets up its own nodes. Without tests, runs all of them:
# test/no_quorum_request.yaml, test/safe_quorum_request.yaml,
# test/each_safe_quorum_request.yaml, then the peer TLS checks in
# test/tls_test.py and the seed resolution checks in test/seed_test.py.

usage() {
    echo "usage: $0 [-n|--no-rebuild] [test ...]" >&2
    exit 2
}

# parse options
rebuild=true
while [ "$#" -gt 0 ]; do
    case $1 in
        -n|--no-rebuild) shift; rebuild=false;;
        -h|--help) usage;;
        -*) echo "unknown option: '$1'" >&2; usage;;
        *) break;; # reached the list of tests
    esac
done

set -o errexit
set -o nounset
set -o pipefail

cd "$(dirname "$0")/.."

if [ "$#" -eq 0 ]; then
    set -- test/no_quorum_request.yaml test/safe_quorum_request.yaml \
        test/each_safe_quorum_request.yaml test/tls_test.py test/seed_test.py
fi

#build Dynomite
if [[ "${rebuild}" == "true" ]]; then
    CFLAGS="-ggdb3 -O0" autoreconf -fvi && ./configure --enable-debug=log && make
else
    echo "not rebuilding Dynomite"
fi

# The tests that don't depend on the cluster run in the first run only.
skip_node_tests=()
for t in "$@"; do
    case ${t} in
        *.py)
            echo "Running ${t}"
            "${t}"
            ;;
        *)
            echo "Running functional tests with ${t}"
            test/cluster_generator.py "${skip_node_tests[@]}" "${t}"
            skip_node_tests=(--skip-node-tests)
            echo "Running functional tests with ${t} and peer TLS"
            test/cluster_generator.py --skip-node-tests --peer-tls "${t}"
            ;;
    esac
done
