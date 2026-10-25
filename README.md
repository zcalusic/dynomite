> **Note:** This is an unofficial fork of [Netflix/dynomite](https://github.com/Netflix/dynomite),
> maintained by [@zcalusic](https://github.com/zcalusic). It is not affiliated with or endorsed by
> Netflix. Upstream is no longer actively maintained, so this fork collects fixes needed to keep
> Dynomite building and running on current systems (newer GCC and Debian releases, memory leak and
> crash fixes). Releases are tagged as `v0.6.22-N`. A Docker image,
> [zcalusic/dynomite](https://hub.docker.com/r/zcalusic/dynomite/), is built from
> [zcalusic/dockerfiles](https://github.com/zcalusic/dockerfiles/tree/master/dynomite). Please
> report issues for this fork here, not upstream.

![dynomite logo](images/dynomite-logo.png)

# Dynomite

[![CI](https://github.com/zcalusic/dynomite/actions/workflows/ci.yml/badge.svg)](https://github.com/zcalusic/dynomite/actions/workflows/ci.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/zcalusic/dynomite.svg)](https://hub.docker.com/r/zcalusic/dynomite/)
[![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](LICENSE)

**Dynomite**, inspired by [Dynamo whitepaper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf), is a thin, distributed dynamo layer for different storage engines and protocols. Currently these include [Redis](https://redis.io) and [Memcached](https://www.memcached.org/).  Dynomite supports multi-datacenter replication and is designed for high availability.

The ultimate goal with Dynomite is to be able to implement high availability and cross-datacenter replication on storage engines that do not inherently provide that functionality. The implementation is efficient, not complex (few moving parts), and highly performant.

## Workflow

All development happens on the `master` branch, the only branch in this fork. Releases are signed tags named `v0.6.22-N`, published as [GitHub releases](https://github.com/zcalusic/dynomite/releases); the Docker image is built from the latest release. Every push to `master` is built and tested by [CI](https://github.com/zcalusic/dynomite/actions/workflows/ci.yml).

## Build

Install the build dependencies (Debian package names):

    $ sudo apt-get install automake gcc git libc6-dev libssl-dev libtool libyaml-dev make patch

Build from a git clone: the version is taken from `git describe`, which needs the history and tags.

To build and install Dynomite, as the Docker image does:

    $ git clone https://github.com/zcalusic/dynomite.git
    $ cd dynomite
    $ autoreconf -fvi
    $ ./configure
    $ make
    $ sudo make install

For a debug build, pass `--enable-debug` to `configure`:

+ `--enable-debug=log`: debug logs enabled, assertions disabled.
+ `--enable-debug=yes`: debug logs enabled, failed assertions are logged.
+ `--enable-debug=full`: debug logs enabled, failed assertions abort Dynomite.

For example, to build in _debug mode_ for use with a debugger:

    $ autoreconf -fvi
    $ CFLAGS="-ggdb3 -O0" ./configure --enable-debug=full
    $ make
    $ src/dynomite -h

## Tests

The unit tests (`src/dyn_test.c`) need no extra dependencies; run them after building:

    $ make check

The functional tests start a local cluster of Dynomite and Redis nodes and compare the results of the same commands against Dynomite and a standalone Redis. Install their dependencies (Debian package names):

    $ sudo apt-get install redis-server python3 python3-plumbum python3-redis python3-requests python3-yaml

Then run them from the repository; this builds Dynomite with debug logs first (`-n` skips the build):

    $ test/run_tests.sh

## Help

    Usage: dynomite [-?hVdDt] [-v verbosity level] [-o output file]
                    [-c conf file] [-p pid file] [-x admin operation]
                    [-m mbuf size (deprecated)]
                    [-M max alloc messages (deprecated)]

    Options:
      -h, --help              : this help
      -V, --version           : show version and exit
      -t, --test-conf         : test configuration for syntax errors and exit
      -d, --daemonize         : run as a daemon
      -D, --describe-stats    : print stats description and exit
      -v, --verbosity=N       : set logging level (default: 5, min: 0, max: 11)
      -o, --output=S          : set logging file (default: stderr)
      -c, --conf-file=S       : set configuration file (default: /usr/local/etc/dynomite.yml)
      -p, --pid-file=S        : set pid file (default: off)
      -x, --admin-operation=N : set admin mode, 1 sends deletes to the local datastore only (default: 0, off)
      -m, --mbuf-size=N       : set size of mbuf chunk in bytes (default: 16384 bytes, deprecated)
      -M, --max-msgs=N        : set max number of messages to allocate (default: 200000, deprecated)

## Configuration

Dynomite is configured through a YAML 1.1 file (YAML 1.1 is not JSON compatible), given with the -c or --conf-file command-line argument on process start. The keys below go under a single top-level pool name, such as `dyn_o_mite:`. Dynomite understands the following keys:

+ **env**: Specify environment of a node.  Currently supports aws and network (for physical datacenter). Defaults to aws.
+ **datacenter**: The name of the datacenter.  Please refer to [architecture document](https://github.com/Netflix/dynomite/wiki/Architecture).
+ **rack**: The name of the rack.  Please refer to [architecture document](https://github.com/Netflix/dynomite/wiki/Architecture).
+ **dyn_listen**: The port that dynomite nodes use to inter-communicate and gossip.
+ **enable_gossip**: enable gossip instead of static tokens (default: false). Gossip is experimental.
+ **gos_interval**: The sleeping time in milliseconds at the end of a gossip round.
+ **tokens**: The token(s) owned by a node.  Currently, we don't support vnode yet so this only works with one token for the time being.
+ **dyn_seed_provider**: A seed provider implementation to provide a list of seed nodes. One of simple_provider (uses dyn_seeds), florida_provider or dns_provider. Defaults to simple_provider.
+ **dyn_seeds**: A list of seed nodes in the format: address:port:rack:dc:tokens (note that vnode is not supported yet)
+ **listen**: The listening address and port (name:port or ip:port) for this server pool.
+ **backlog**: The TCP listen backlog for the listening sockets. Defaults to 512.
+ **hash**: The hash function used to map keys to tokens. One of one_at_a_time, md5, crc16, crc32, crc32a, fnv1_64, fnv1a_64, fnv1_32, fnv1a_32, hsieh, murmur or jenkins. Defaults to murmur.
+ **hash_tag**: A two-character string, such as `{}`, that selects the part of a key to hash: when a key contains both characters, only the part between them is hashed, so related keys can be kept on the same node. Not set by default.
+ **timeout**: The timeout value in msec that we wait for to establish a connection to the server or receive a response from a server. Defaults to 5000 msec.
+ **preconnect**: A boolean value that controls if dynomite should preconnect to all the servers in this pool on process start. Defaults to true.
+ **data_store**: An integer value that controls if a server pool speaks redis (0) or memcached (1) or other protocol. Defaults to redis (0).
+ **auto_eject_hosts**: A boolean value that controls if server should be ejected temporarily when it fails consecutively server_failure_limit times. See [liveness recommendations](notes/recommendation.md#liveness) for information. Defaults to true.
+ **server_retry_timeout**: The timeout value in msec to wait for before retrying on a temporarily ejected server, when auto_eject_hosts is set to true. Defaults to 10000 msec.
+ **server_failure_limit**: The number of consecutive failures on a server that would lead to it being temporarily ejected when auto_eject_hosts is set to true. Defaults to 3.
+ **servers**: A list of local server address, port and weight (name:port:weight or ip:port:weight) for this server pool. Currently, there is just one.
+ **secure_server_option**: Encrypted communication. Must be one of 'none', 'rack', 'datacenter', or 'all'. ```datacenter``` means all communication between datacenters is encrypted but within a datacenter it is not. ```rack``` means all communication between racks and regions is encrypted however communication between nodes within the same rack is not encrypted. ```all``` means all communication between all nodes is encrypted. And ```none``` means none of the communication is encrypted. 
+ **pem_key_file**: The RSA private key file used for encrypted communication between nodes. Defaults to conf/dynomite.pem, relative to the working directory.
+ **recon_key_file**, **recon_iv_file**: The key and IV files used to encrypt data reconciliation (entropy) traffic. Default to conf/recon_key.pem and conf/recon_iv.pem, relative to the working directory.
+ **read_consistency**, **write_consistency**: The consistency level for reads and writes: DC_ONE, DC_QUORUM, DC_SAFE_QUORUM or DC_EACH_SAFE_QUORUM (case-insensitive). Default to DC_ONE. Please refer to [consistency document](https://github.com/Netflix/dynomite/wiki/Consistency). Except for DC_EACH_SAFE_QUORUM, both can also be changed at runtime through the stats_listen REST endpoint (`/set_consistency/read/<level>` and `/set_consistency/write/<level>`).
+ **read_repairs_enabled**: A boolean value that enables read repairs, which update replicas found with stale data during a read; they don't run with DC_ONE. Defaults to false. Please refer to [read repair document](https://github.com/Netflix/dynomite/wiki/Read-Repair). Can also be changed at runtime through the stats_listen REST endpoint (`/read_repairs/enable` and `/read_repairs/disable`).
+ **stats_listen**: The address and port number for the REST endpoint and for accessing statistics.
+ **stats_interval**: set stats aggregation interval in msec (default: 30000 msec).
+ **mbuf_size**: size of mbuf chunk in bytes (default: 16384 bytes).
+ **max_msgs**: max number of messages to allocate (default: 200000).
+ **datastore_connections**: Maximum number of connections to the local datastore. Defaults to 1.
+ **local_peer_connections**: Maximum number of connections to a local DC peer. Defaults to 1.
+ **remote_peer_connections**: Maximum number of connections to a remote DC peer. Defaults to 1.
+ **dyn_port**: Accepted, but currently has no effect; the inter-node port comes from dyn_listen.
+ **dyn_connections**, **dyn_read_timeout**, **dyn_write_timeout**: Accepted, but currently have no effect.
+ **conn_msg_rate**: The maximum number of messages per second sent on each connection to a node in a remote datacenter. Defaults to 50000.

The keys **client_connections**, **distribution** and **server_connections** are still accepted for compatibility, but are ignored.

See [conf/dynomite.yml](conf/dynomite.yml) for an example; [conf/](conf/) has more, such as [conf/redis_single.yml](conf/redis_single.yml).

Finally, to make writing syntactically correct configuration files easier, dynomite provides a command-line argument -t or --test-conf that can be used to test the YAML configuration file for any syntax error.


## License

Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
