If you are deploying dynomite in your production environment, here are a few recommendations that might be worth considering.

## Log Level

A plain build (`./configure`, as the Docker image and CI use) only logs warnings, errors and a few startup lines, whatever the verbosity. For more, build dynomite with `--enable-debug=log` (or `--enable-debug=full`, which also turns on assertions) and run it with verbosity LOG_INFO (`-v 6` or `--verbosity=6`). The cost is small: every log line below the chosen level only costs an if condition.

At LOG_INFO, dynomite logs the life cycle of every client and server connection and other important events, for example a client request that times out on the datastore:

    [2026-10-26 16:48:32.410] proxy_accept:203 <CONN_PROXY 0x55cab1af8ef0 8 listening on '0.0.0.0:8102'> accepted <CONN_CLIENT 0x55cab1b00540 10 from '127.0.0.1:58120'>
    [2026-10-26 16:48:32.811] core_timeout:460 <REQ 0x55cab1b007f0 4:0::0 REQ_REDIS_GET, len:22> on <CONN_SERVER 0x55cab1aeec10 7 to '127.0.0.1:22122:1'> timedout, timeout was 400
    [2026-10-26 16:48:32.811] core_close:396 close <CONN_SERVER 0x55cab1aeec10 7 to '127.0.0.1:22122:1'> on event FF00 eof 0 done 0 rb 5 sb 53: Connection timed out
    [2026-10-26 16:48:32.811] server_close:255 close <CONN_SERVER 0x55cab1aeec10 7 to '127.0.0.1:22122:1'> Dropped 1 outqueue & 0 inqueue requests
    [2026-10-26 16:48:32.811] core_close:396 close <CONN_CLIENT 0x55cab1b00540 10 from '127.0.0.1:58120'> on event 00FF eof 1 done 1 rb 22 sb 44
    [2026-10-26 16:48:33.813] conn_connect:283 <CONN_SERVER 0x55cab1b00540 7 to '127.0.0.1:22122:1'> connecting.....
    [2026-10-26 16:48:33.813] server_connected:297 <CONN_SERVER 0x55cab1b00540 7 to '127.0.0.1:22122:1'> connected

Use `-o <file>` to log to a file instead of stderr.

## Liveness

When a connection to the datastore or to a peer fails, dynomite reconnects in the background: it waits 1 second, then doubles the wait after each failed attempt. The wait is capped by `server_retry_timeout:` (in msec, applied in whole seconds) for the datastore and at 10 seconds for peers. Dynomite itself does not retry a request, so clients should retry requests that fail (see [Error Response](#error-response)).

Dynomite does not eject failing servers from the hash ring. The keys `auto_eject_hosts:` and `server_failure_limit:` are accepted for compatibility, but ignored.

## Timeout

`timeout:` (default 5000 msec, at least 1) limits how long dynomite waits for the datastore to answer a request. It is a good idea to set it, rather than relying only on client-side timeouts: a request that timed out on the client would otherwise still be pending between dynomite and the datastore, and a client retry adds to the load.

When a request times out, dynomite answers it with `-ERR Storage: Datastore timed out` and closes the connection to the datastore, so the other requests queued on that connection get the same error. Requests then fail until the connection is back (see [Liveness](#liveness)), which can take a few seconds. Choose a `timeout:` well above the datastore's normal response times, so that a single slow command doesn't cut off everything else.

## Error Response

When dynomite can't serve a request, it answers with an error of the form `-ERR <source> <description>` (redis) or `SERVER_ERROR <source> <description>` (memcache), where the source is `Storage:` (the local datastore), `Peer:` (another dynomite node) or `Dynomite:` (dynomite itself). For example:

+ `-ERR Storage: Datastore is not connected` when dynomite has no connection to the datastore: it is down, or dynomite hasn't reconnected yet (see [Liveness](#liveness)),
+ `-ERR Storage: Datastore refused connection` when the connection to the datastore failed while the request was on it (the datastore closed or reset it),
+ `-ERR Storage: Datastore timed out` when a request to it timed out (see [Timeout](#timeout)); a request forwarded to another node that doesn't answer in time gets `-ERR Peer: Peer Node timed out`,
+ `-ERR Peer: Peer Node is down` when the node that owns the key can't be reached,
+ `-ERR Dynomite: Failed to achieve Quorum` when a quorum read or write didn't get enough replies.

Clients should treat these as transient failures and retry the request.

## mbuf Size

Dynomite keeps requests and responses in mbufs, buffers of `mbuf_size:` bytes (default 16384; 512 to 512000, a multiple of 16). Connections take mbufs as they need them, so smaller mbufs use less memory when there are many concurrent client connections, while larger mbufs read and write more data per system call.

## Maximum Key Length

The memcache protocol limits keys to 250 characters. Redis has no such limit, but dynomite needs every key to fit in one mbuf, so a redis key can be at most `mbuf_size:` minus 57 bytes long (16327 bytes with the default mbuf size). A request with a longer key gets an error and isn't run, and the connection stays open: `-ERR Dynomite: Key is too long, the limit is 16327 bytes` (redis) or `CLIENT_ERROR Dynomite: Key is too long, the limit is 250 bytes` (memcache; a `noreply` request gets no reply). If your redis keys can be long, choose a larger `mbuf_size:`. Requests between nodes are still parsed against the receiving node's mbuf size, so give every node the same `mbuf_size:`.

## Hash Tags

[Hash Tags](http://antirez.com/post/redis-presharding.html) enable you to use part of the key for calculating the hash. When the hash tag is present, we use the part of the key within the tag as the key to be used for consistent hashing. Otherwise, we use the full key as is. Hash tags enable you to map different keys to the same node as long as the part of the key within the tag is the same.

For example, the configuration below specifies a two-character hash_tag string - "{}". This means that keys "user:{user1}:ids" and "user:{user1}:tweets" map to the same node because we compute the hash on "user1". For a key like "user:user1:ids", we use the entire string "user:user1:ids" to compute the hash and it may map to a different node.

    dyn_o_mite:
      listen: 127.0.0.1:8102
      dyn_listen: 127.0.0.1:8101
      stats_listen: 127.0.0.1:22222
      hash: fnv1a_64
      hash_tag: "{}"
      timeout: 400
      servers:
      - 127.0.0.1:22122:1
      tokens: 437425602

Commands with several keys, such as MGET, MSET and DEL, are split by key and sent to the nodes that own each key. Commands that must run on one node, such as EVAL with several keys or SUNIONSTORE, need all their keys to hash to the same node, which a common hash tag ensures (see [redis.md](redis.md)).

## Graphing Datastore Failures

To follow the health of the datastore over time, graph these stats exposed by dynomite:

- **server_eof** is incremented when the server closes the connection normally, which should not happen because we use persistent connections.
- **server_timedout** is incremented when the connection / request to the server timed out.
- **server_err** is incremented for any other kind of error.
- **server_ejects** is incremented on every failure of a datastore connection, and **server_ejected_at** holds the time of the last one. Despite their names, no server is ejected.

## Several Datastore Connections

With `datastore_connections:` above 1, dynomite opens several connections to the local datastore, but each client connection always uses the same one, so pipelined requests from one client reach the local datastore in order.
