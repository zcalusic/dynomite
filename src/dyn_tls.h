/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

/*
 * Mutual TLS 1.3 between peers (peer_tls in the configuration): the dnode
 * listener and the outgoing peer connections. Every peer presents a cert
 * issued by the cluster CA, in both directions; nothing else gets to send a
 * dnode message. The client port, the stats port and the datastore
 * connection stay plaintext.
 *
 * OpenSSL is kept behind these functions: the rest of dynomite only sees
 * the opaque tls_ctx (one per process) and tls_conn (one per peer conn).
 */

#ifndef _DYN_TLS_H_
#define _DYN_TLS_H_

#include <stdbool.h>
#include <sys/types.h>

#include "dyn_types.h"

struct array;
struct conf_pool;
struct conn;
struct context;
struct tls_ctx;
struct tls_conn;

/* a handshake (TCP connect included for outgoing conns) must finish in this */
#define TLS_HANDSHAKE_TIMEOUT_MS 10000

/* warn daily when the node cert expires in fewer days than this */
#define TLS_CERT_WARN_DAYS 14

struct tls_ctx *tls_ctx_create(struct conf_pool *cp);
void tls_ctx_destroy(struct tls_ctx *tls);
bool tls_ctx_cert_warned(struct tls_ctx *tls);
int64_t tls_cert_days_left(struct tls_ctx *tls);
void tls_schedule_expiry_check(struct context *ctx);

rstatus_t tls_conn_start(struct context *ctx, struct conn *conn,
                         bool is_server);
bool tls_conn_handshaking(struct conn *conn);
rstatus_t tls_conn_handshake(struct context *ctx, struct conn *conn);
ssize_t tls_conn_recv(struct conn *conn, void *buf, size_t size);
ssize_t tls_conn_sendv(struct conn *conn, struct array *sendv, size_t nsend);
void tls_conn_close(struct conn *conn);

/* for dynomite-test */
rstatus_t tls_test_handshake(struct tls_ctx *client, struct tls_ctx *server,
                             const char **detail);

#endif
