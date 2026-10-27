> **Note:** This is an unofficial fork of [Netflix/dynomite](https://github.com/Netflix/dynomite),
> maintained by [@zcalusic](https://github.com/zcalusic). It is not affiliated with or endorsed by
> Netflix. Upstream is no longer actively maintained, so this fork collects fixes needed to keep
> Dynomite building and running on current systems (newer GCC and Debian releases, memory leak and
> crash fixes). Releases are tagged `vX.Y.Z`, starting with `v0.9.0`. A Docker image,
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

All development happens on the `master` branch, the only branch in this fork. Releases are signed tags named `vX.Y.Z` (`v0.9.0`, `v0.9.1`, ...; before `v0.9.0` they were `v0.6.22-N`), published as [GitHub releases](https://github.com/zcalusic/dynomite/releases); the Docker image is built from the latest release. Every push to `master` is built and tested by [CI](https://github.com/zcalusic/dynomite/actions/workflows/ci.yml).

## Build

Install the build dependencies (Debian package names):

    $ sudo apt-get install automake gcc git libc6-dev libssl-dev libyaml-dev make

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

    $ sudo apt-get install openssl redis-server python3 python3-plumbum python3-redis python3-requests python3-yaml

Then run them from the repository; this builds Dynomite with debug logs first (`-n` skips the build). Each cluster runs plaintext and with mutual TLS between its nodes (certificates are made on the fly with `openssl`), and `test/tls_test.py` checks what the TLS peer port accepts and rejects:

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

Signals: SIGTERM (`docker stop`, `kubectl delete pod`, systemd) and SIGINT make Dynomite exit cleanly at once, with exit status 0 and the pid file removed. SIGHUP reopens the log file, SIGTTIN and SIGTTOU raise and lower the logging level.

## Configuration

Dynomite is configured through a YAML 1.1 file (YAML 1.1 is not JSON compatible), given with the -c or --conf-file command-line argument on process start. The keys below go under a single top-level pool name, such as `dyn_o_mite:`. Dynomite understands the following keys:

+ **env**: Specify environment of a node.  Currently supports aws and network (for physical datacenter). Defaults to aws.
+ **datacenter**: The name of the datacenter.  Please refer to [architecture document](https://github.com/Netflix/dynomite/wiki/Architecture).
+ **rack**: The name of the rack.  Please refer to [architecture document](https://github.com/Netflix/dynomite/wiki/Architecture).
+ **dyn_listen**: The port that dynomite nodes use to inter-communicate and gossip.
+ **enable_gossip**: enable gossip instead of static tokens (default: false). Gossip is experimental.
+ **gos_interval**: The sleeping time in milliseconds at the end of a gossip round. Defaults to 30000; must be at least 1.
+ **tokens**: The token(s) owned by a node.  Currently, we don't support vnode yet so this only works with one token for the time being. A token is a number from 0 to 4294967295; anything else is a configuration error.
+ **dyn_seed_provider**: A seed provider implementation to provide a list of seed nodes. One of simple_provider (uses dyn_seeds), florida_provider or dns_provider. Defaults to simple_provider.
+ **dyn_seeds**: A list of seed nodes in the format: address:port:rack:dc:tokens (note that vnode is not supported yet)
+ **listen**: The listening address and port (name:port or ip:port) for this server pool.
+ **backlog**: The TCP listen backlog for the listening sockets. Defaults to 512; must be at least 1.
+ **hash**: The hash function used to map keys to tokens. One of one_at_a_time, md5, crc16, crc32, crc32a, fnv1_64, fnv1a_64, fnv1_32, fnv1a_32, hsieh, murmur or jenkins. Defaults to murmur.
+ **hash_tag**: A two-character string, such as `{}`, that selects the part of a key to hash: when a key contains both characters, only the part between them is hashed, so related keys can be kept on the same node. Not set by default.
+ **timeout**: The timeout value in msec that we wait for to establish a connection to the server or receive a response from a server. Defaults to 5000 msec; must be at least 1.
+ **data_store**: An integer value that controls if a server pool speaks redis (0) or memcached (1). Defaults to redis (0).
+ **server_retry_timeout**: The longest wait in msec between attempts to reconnect to the server: the wait starts at 1 second and doubles after each failure, up to this limit in whole seconds (rounded down), so values below 2000, including 0, reconnect every second. Defaults to 10000 msec.
+ **servers**: A list of local server address, port and weight (name:port:weight or ip:port:weight) for this server pool. Exactly one is supported; a second entry is a configuration error.
+ **read_consistency**, **write_consistency**: The consistency level for reads and writes: DC_ONE, DC_QUORUM, DC_SAFE_QUORUM or DC_EACH_SAFE_QUORUM (case-insensitive). Default to DC_ONE. Please refer to [consistency document](https://github.com/Netflix/dynomite/wiki/Consistency). Both can also be changed at runtime through the stats_listen REST endpoint (`/set_consistency/read/<level>` and `/set_consistency/write/<level>`); the new level applies to client connections opened after the change.
+ **read_repairs_enabled**: A boolean value that enables read repairs, which update replicas found with stale data during a read; they don't run with DC_ONE. Defaults to false. Please refer to [read repair document](https://github.com/Netflix/dynomite/wiki/Read-Repair). Can also be changed at runtime through the stats_listen REST endpoint (`/read_repairs/enable` and `/read_repairs/disable`).
+ **stats_listen**: The address and port number for the REST endpoint and for accessing statistics. `GET /help` lists the commands. Paths must match exactly; an unknown path gets 404 Not Found, an invalid argument (log level, timeout factor, consistency level, missing peer name) 400 Bad Request, an unknown peer 404, and a method other than GET 405 Method Not Allowed.
+ **stats_interval**: set stats aggregation interval in msec (default: 30000 msec; at least 1).
+ **mbuf_size**: size of mbuf chunk in bytes (default: 16384 bytes; 512 to 512000, a multiple of 16).
+ **max_msgs**: max number of messages to allocate (default: 200000; 100000 to 1000000).
+ **datastore_connections**: Maximum number of connections to the local datastore. Defaults to 1; 1 to 255.
+ **local_peer_connections**: Maximum number of connections to a local DC peer. Defaults to 1; 1 to 255.
+ **remote_peer_connections**: Maximum number of connections to a remote DC peer. Defaults to 1; 1 to 255.
+ **conn_msg_rate**: The maximum number of messages per second sent on each connection to a node in a remote datacenter. Defaults to 50000; must be at least 1.
+ **peer_tls**: A boolean value that turns on mutual TLS 1.3 for the traffic between nodes, in both directions; see [Securing traffic between nodes](#securing-traffic-between-nodes). Defaults to false.
+ **peer_tls_cert**: The node's certificate in PEM format, followed by any intermediate CA certificates. Required with `peer_tls: true`.
+ **peer_tls_key**: The node's private key in PEM format, unencrypted. Required with `peer_tls: true`.
+ **peer_tls_ca**: The cluster CA certificate(s) in PEM format, the only certificates peers are trusted with. Required with `peer_tls: true`.

The keys **client_connections**, **distribution**, **server_connections**, **dyn_port**, **dyn_connections**, **dyn_read_timeout**, **dyn_write_timeout**, **recon_key_file**, **recon_iv_file**, **auto_eject_hosts**, **server_failure_limit**, **preconnect** and **pem_key_file** are still accepted for compatibility, but are ignored. The inter-node port comes from **dyn_listen**. Dynomite always connects to the datastore and to its peers at startup.

The built-in encryption of traffic between nodes was removed: it crashed with RSA keys over 1024 bits, lost writes with `all`, still accepted unencrypted messages, and gave no integrity or forward secrecy. Mutual TLS replaces it, see the next section. **secure_server_option** is still accepted with the value `none` and ignored; any other value is a configuration error, so that a node doesn't quietly run unencrypted when its configuration asks for encryption. All nodes of a cluster that used encryption have to switch to `none` together: a node closes peer connections that send encrypted messages.

See [conf/dynomite.yml](conf/dynomite.yml) for an example; [conf/](conf/) has more, such as [conf/redis_single.yml](conf/redis_single.yml).

Finally, to make writing syntactically correct configuration files easier, dynomite provides a command-line argument -t or --test-conf that can be used to test the YAML configuration file for any syntax error. With `peer_tls: true` it also loads the certificate, key and CA files and reports what is wrong with them.

## Securing traffic between nodes

With `peer_tls: true`, every connection between nodes (the `dyn_listen` port, gossip included) is mutual TLS 1.3: each node presents its certificate when connecting and when accepting, and both sides accept only certificates issued by the CA(s) in `peer_tls_ca`. A connection without a valid client certificate is closed before any message is read, so nothing but the cluster's own nodes can talk to the peer port, whatever network it is on. Nodes are verified by the CA only: the names and addresses in the certificates are not checked, so one certificate can be reused on another node, and a node can be reached under any address. OpenSSL's defaults apply otherwise: the TLS 1.3 ciphersuites, and the hybrid post-quantum key exchange (X25519MLKEM768) on OpenSSL 3.5 and later. Session resumption is off, every connection does a full handshake. A handshake that does not finish within 10 seconds is closed.

What stays unencrypted: the client port (`listen`), the stats port (`stats_listen`) and the connection to the datastore. Keep those on a trusted network.

Use one private CA per cluster, never a shared company CA and never a public one: anything that CA signs can join the cluster. OpenSSL is enough to run it; this makes a CA and a certificate for one node, both Ed25519 (ECDSA P-256 or RSA 2048+ work the same way), the certificate valid for a year:

    $ openssl req -x509 -newkey ed25519 -nodes -keyout ca.key -out ca.pem -days 3650 -subj /CN=dynomite-ca
    $ openssl req -newkey ed25519 -nodes -keyout node1.key -out node1.csr -subj /CN=node1
    $ printf 'subjectAltName=DNS:node1,IP:10.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n' > node1.ext
    $ openssl x509 -req -in node1.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 365 -extfile node1.ext -out node1.pem

Each certificate needs the `serverAuth` and `clientAuth` extended key usages (or none at all), since the same certificate serves both directions. Keep `ca.key` off the nodes; give each node its `node1.pem`, `node1.key` (mode 0600) and `ca.pem`:

    peer_tls: true
    peer_tls_cert: /etc/dynomite/node1.pem
    peer_tls_key: /etc/dynomite/node1.key
    peer_tls_ca: /etc/dynomite/ca.pem

In the Docker image, mount the files read-only and point the configuration at them, for example `-v /etc/dynomite/tls:/tls:ro` with `peer_tls_cert: /tls/node1.pem` and so on.

All nodes of a cluster switch together: a node with `peer_tls: true` closes plaintext peers, and a node with it off closes TLS peers, so a mixed cluster doesn't replicate until the last node is switched. When new certificates are needed, replace the files and restart the node (a rolling restart, one node at a time, keeps the cluster serving); there is no reload. To rotate the CA, put the old and the new CA certificate into `peer_tls_ca` on every node and restart, then reissue the node certificates from the new CA, and finally drop the old CA from the file.

At startup, a node checks its own certificate the way its peers will and logs a warning when it doesn't verify with `peer_tls_ca`, has expired or is not valid yet; it starts anyway, the handshake failures show up on the peers. It warns daily when the certificate expires within 14 days. Missing or unreadable files, a key that doesn't match the certificate or a CA file without a certificate are configuration errors and the node doesn't start. The stats REST endpoint (`/info`) has `peer_tls_cert_days_left`, and the counters `peer_tls_handshakes`, `peer_tls_handshake_errors` and `peer_tls_plaintext_rejected`; every failed handshake is logged with the peer's address and the reason. To check a node from outside, connect with a node certificate (it must fail without one):

    $ openssl s_client -connect node1:8101 -CAfile ca.pem -cert node2.pem -key node2.key


## License

Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
