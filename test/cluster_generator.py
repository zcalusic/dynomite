#!/usr/bin/env python3
from contextlib import ExitStack
from urllib.request import urlopen
import argparse
import json
import random
import sys
import yaml

from plumbum import local

from dyno_cluster import DynoCluster
from func_test import comparison_test
from utils import generate_ips, setup_temp_dir, wait_for
from redis_node import RedisNode

REDIS_PORT = 1212
STATS_PORT = 22222

def main():
    parser = argparse.ArgumentParser(
        description='Autogenerates a Dynomite cluster and runs functional ' +
            'tests against it')
    parser.add_argument('request_file', default='test/no_quorum_request.yaml',
        help='YAML file describing desired cluster', nargs='?')
    parser.add_argument('--peer-tls', action='store_true',
        help='mutual TLS between the nodes, with certificates made on the fly')
    args = parser.parse_args()

    # Set up a temporary directory to store logs and configs for this cluster.
    temp = setup_temp_dir()

    # Generate IP addresses to be used by the nodes we will create.
    ips = generate_ips()

    # Create a standalone Redis node.
    standalone_redis_ip = next(ips)
    standalone_redis = RedisNode(standalone_redis_ip, REDIS_PORT)

    # Create a Dynomite cluster.
    dynomite_cluster = DynoCluster(args.request_file, ips, peer_tls=args.peer_tls)

    with ExitStack() as stack:
        # Make sure to change the working directory to the temp dir before running the
        # tests.
        stack.enter_context(local.cwd(temp))
        # Launch the standalone Redis node and the dynomite cluster.
        stack.enter_context(standalone_redis)
        stack.enter_context(dynomite_cluster)

        # Wait for the above nodes to start and connect to each other.
        wait_for('the standalone redis', standalone_redis.is_up, 30)
        dynomite_cluster.wait_until_ready()

        # Run all the functional comparison tests.
        comparison_test(standalone_redis, dynomite_cluster, False)

        random_node = random.choice(dynomite_cluster.nodes)
        stats_url = 'http://{}:{}/info'.format(random_node.ip, STATS_PORT)
        json.loads(urlopen(stats_url).read().decode('ascii'))

if __name__ == '__main__':
    sys.exit(main())
