#!/bin/bash
#
# Build Dynomite and run the functional tests against generated clusters.
#
# usage: test/run_tests.sh [-n|--no-rebuild] [request_file ...]
#
# Without request files, runs test/no_quorum_request.yaml and
# test/safe_quorum_request.yaml.

usage() {
    echo "usage: $0 [-n|--no-rebuild] [request_file ...]" >&2
    exit 2
}

# parse options
rebuild=true
while [ "$#" -gt 0 ]; do
    case $1 in
        -n|--no-rebuild) shift; rebuild=false;;
        -h|--help) usage;;
        -*) echo "unknown option: '$1'" >&2; usage;;
        *) break;; # reached the list of request files
    esac
done

set -o errexit
set -o nounset
set -o pipefail

cd "$(dirname "$0")/.."

if [ "$#" -eq 0 ]; then
    set -- test/no_quorum_request.yaml test/safe_quorum_request.yaml
fi

#build Dynomite
if [[ "${rebuild}" == "true" ]]; then
    CFLAGS="-ggdb3 -O0" autoreconf -fvi && ./configure --enable-debug=log && make
else
    echo "not rebuilding Dynomite"
fi

for request_file in "$@"; do
    echo "Running functional tests with ${request_file}"
    test/cluster_generator.py "${request_file}"
done
