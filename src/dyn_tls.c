/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

/*
 * Mutual TLS 1.3 between peers, see dyn_tls.h.
 *
 * All peer I/O happens on the main thread, through conn_recv_data() and
 * conn_sendv_data(), which call tls_conn_recv() and tls_conn_sendv() for
 * a conn with TLS. The handshake runs from core_core() before the normal
 * read/write dispatch, driven by the same epoll events.
 *
 * The event loop is edge-triggered, which shapes the I/O here:
 *
 * - Reads go on until SSL_read wants more from the socket: OpenSSL hands
 *   out one record at a time and may still hold decrypted bytes, so a short
 *   read doesn't mean the socket is drained, unlike with plain read().
 * - Writes go on until everything is written or the socket is full: the
 *   staging buffer makes one 16 KB record per SSL_write, and stopping after
 *   one record would wait for an EPOLLOUT edge that never comes.
 * - A record that SSL_write could not push out completely is retried with
 *   the same buffer and length (OpenSSL insists on that); the iovecs that
 *   msg_send_chain() rebuilds start with exactly those bytes, since the
 *   partially sent message stays first in the queue at the same position.
 *
 * TLS 1.3 has no renegotiation, session tickets are off and OpenSSL never
 * sends a KeyUpdate on its own, so after the handshake SSL_read only ever
 * wants to read and SSL_write only ever wants to write. The other two
 * cases close the connection, which reconnects.
 */

#include "dyn_tls.h"

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <sys/socket.h>
#include <time.h>

#include "dyn_conf.h"
#include "dyn_core.h"
#include "dyn_task.h"

/* one TLS record of plaintext, the staging buffer for writes */
#define TLS_RECORD_MAX SSL3_RT_MAX_PLAIN_LENGTH

/* the first byte of a TLS handshake record; dnode messages start with ' ' */
#define TLS_HANDSHAKE_RECORD 0x16

#define TLS_EXPIRY_CHECK_MS (24 * 60 * 60 * 1000)

struct tls_ctx {
  SSL_CTX *ssl_ctx;
  time_t not_after;         /* the node cert's expiry */
  bool cert_warned;         /* the startup self-check found a problem */
  struct task *expiry_task; /* the daily expiry check */
};

struct tls_conn {
  SSL *ssl;
  struct task *hs_timeout; /* closes a handshake that takes too long */
  uint8_t *wbuf;           /* staging buffer, one record */
  size_t wpending;         /* bytes in wbuf SSL_write has not taken yet */
  unsigned handshaking : 1;
  unsigned is_server : 1;
  unsigned peeked : 1; /* server side: the first byte was checked */
  unsigned fatal : 1;  /* SSL_shutdown must not run after a fatal error */
};

/*
 * The OpenSSL error as text, and the queue cleared. The queue is per thread
 * and all TLS work is on the main thread, so what is in it belongs to the
 * call that just failed. The earliest entry is the specific one ("No such
 * file or directory"), the later ones wrap it ("BIO lib").
 */
static const char *tls_last_error(void) {
  static char buf[256];
  unsigned long err = ERR_get_error();

  ERR_clear_error();
  if (err == 0) {
    return "unknown error";
  }
  const char *reason = ERR_reason_error_string(err);
  if (reason != NULL) {
    return reason;
  }
  ERR_error_string_n(err, buf, sizeof(buf));
  return buf;
}

/* a plain message for the usual mistakes before OpenSSL opens the file */
static bool tls_file_readable(const char *what, const char *path) {
  if (access(path, R_OK) != 0) {
    log_error("peer_tls: can't read %s '%s': %s", what, path, strerror(errno));
    return false;
  }
  return true;
}

static bool tls_time(const ASN1_TIME *t, time_t *out) {
  struct tm tm;

  if (ASN1_TIME_to_tm(t, &tm) != 1) {
    return false;
  }
  *out = timegm(&tm);
  return true;
}

/*
 * Check the node's own cert the way a peer will: the chain against the CAs
 * in peer_tls_ca, and the validity period. Problems are warnings: the node
 * still starts, and the peers' rejections show in the logs and counters.
 */
static void tls_self_check(struct tls_ctx *tls, struct conf_pool *cp) {
  X509 *cert = SSL_CTX_get0_certificate(tls->ssl_ctx);
  STACK_OF(X509) *chain = NULL;
  X509_STORE_CTX *vc;

  SSL_CTX_get0_chain_certs(tls->ssl_ctx, &chain);

  vc = X509_STORE_CTX_new();
  if (vc == NULL ||
      X509_STORE_CTX_init(vc, SSL_CTX_get_cert_store(tls->ssl_ctx), cert,
                          chain) != 1) {
    log_warn("peer_tls: can't verify '%.*s': %s", cp->peer_tls_cert.len,
             cp->peer_tls_cert.data, tls_last_error());
    X509_STORE_CTX_free(vc);
    tls->cert_warned = true;
    return;
  }

  if (X509_verify_cert(vc) != 1) {
    log_warn(
        "peer_tls: '%.*s' does not verify with the CAs in '%.*s': %s; "
        "peers will reject this node",
        cp->peer_tls_cert.len, cp->peer_tls_cert.data, cp->peer_tls_ca.len,
        cp->peer_tls_ca.data,
        X509_verify_cert_error_string(X509_STORE_CTX_get_error(vc)));
    tls->cert_warned = true;
  }
  X509_STORE_CTX_free(vc);
  ERR_clear_error();

  time_t not_before;
  time_t now = time(NULL);
  if (tls_time(X509_get0_notBefore(cert), &not_before) && not_before > now) {
    log_warn(
        "peer_tls: '%.*s' is not valid yet (check the clock); peers "
        "will reject this node",
        cp->peer_tls_cert.len, cp->peer_tls_cert.data);
    tls->cert_warned = true;
  }
}

static void tls_log_expiry(struct tls_ctx *tls, struct conf_pool *cp) {
  int64_t days = tls_cert_days_left(tls);

  if (days < 0) {
    log_error(
        "peer_tls: '%.*s' has expired; peers reject this node until "
        "it is replaced",
        cp->peer_tls_cert.len, cp->peer_tls_cert.data);
    tls->cert_warned = true;
  } else if (days == 0) {
    log_warn(
        "peer_tls: '%.*s' expires within a day; replace it and restart "
        "before then",
        cp->peer_tls_cert.len, cp->peer_tls_cert.data);
  } else if (days < TLS_CERT_WARN_DAYS) {
    log_warn("peer_tls: '%.*s' expires in %" PRId64
             " days; replace it and "
             "restart before then",
             cp->peer_tls_cert.len, cp->peer_tls_cert.data, days);
  }
}

static void tls_expiry_task(void *arg) {
  struct context *ctx = arg;
  struct tls_ctx *tls = ctx->tls;

  tls->expiry_task = NULL;
  tls_log_expiry(tls, &ctx->cf->pool);
  tls_schedule_expiry_check(ctx);
}

void tls_schedule_expiry_check(struct context *ctx) {
  struct tls_ctx *tls = ctx->tls;

  if (tls == NULL) {
    return;
  }
  ASSERT(tls->expiry_task == NULL);
  tls->expiry_task = schedule_task_1(tls_expiry_task, ctx, TLS_EXPIRY_CHECK_MS);
}

/*
 * The process-wide context: TLS 1.3 only, the node cert and key, the
 * cluster CA(s) as the only trust anchors, and a peer cert required in both
 * directions. File and key problems refuse to start; the self-check only
 * warns.
 */
struct tls_ctx *tls_ctx_create(struct conf_pool *cp) {
  struct tls_ctx *tls;
  SSL_CTX *sc;
  const char *cert = (const char *)cp->peer_tls_cert.data;
  const char *key = (const char *)cp->peer_tls_key.data;
  const char *ca = (const char *)cp->peer_tls_ca.data;

  ASSERT(cp->peer_tls);

  tls = dn_zalloc(sizeof(*tls));
  if (tls == NULL) {
    return NULL;
  }

  ERR_clear_error();
  sc = SSL_CTX_new(TLS_method());
  if (sc == NULL) {
    log_error("peer_tls: creating the TLS context failed: %s",
              tls_last_error());
    dn_free(tls);
    return NULL;
  }
  tls->ssl_ctx = sc;

  if (SSL_CTX_set_min_proto_version(sc, TLS1_3_VERSION) != 1 ||
      SSL_CTX_set_max_proto_version(sc, TLS1_3_VERSION) != 1) {
    log_error("peer_tls: TLS 1.3 is not available: %s", tls_last_error());
    goto error;
  }

  /* a peer that dies without close_notify is an eof, not an error */
  SSL_CTX_set_options(sc, SSL_OP_IGNORE_UNEXPECTED_EOF);
  /* give the record buffers back between messages; conns are long-lived */
  SSL_CTX_set_mode(sc, SSL_MODE_RELEASE_BUFFERS);
  /* no resumption: every connection does a full handshake */
  SSL_CTX_set_num_tickets(sc, 0);
  SSL_CTX_set_session_cache_mode(sc, SSL_SESS_CACHE_OFF);

  if (!tls_file_readable("peer_tls_cert", cert) ||
      !tls_file_readable("peer_tls_key", key) ||
      !tls_file_readable("peer_tls_ca", ca)) {
    goto error;
  }
  if (SSL_CTX_use_certificate_chain_file(sc, cert) != 1) {
    log_error("peer_tls: loading peer_tls_cert '%s' failed: %s", cert,
              tls_last_error());
    goto error;
  }
  if (SSL_CTX_use_PrivateKey_file(sc, key, SSL_FILETYPE_PEM) != 1) {
    log_error("peer_tls: loading peer_tls_key '%s' failed: %s", key,
              tls_last_error());
    goto error;
  }
  if (SSL_CTX_check_private_key(sc) != 1) {
    log_error(
        "peer_tls: peer_tls_key '%s' does not match peer_tls_cert "
        "'%s': %s",
        key, cert, tls_last_error());
    goto error;
  }
  if (SSL_CTX_load_verify_file(sc, ca) != 1) {
    log_error("peer_tls: loading peer_tls_ca '%s' failed: %s", ca,
              tls_last_error());
    goto error;
  }

  /* the CA in peer_tls_ca is the anchor, even if it isn't self-signed */
  X509_VERIFY_PARAM_set_flags(SSL_CTX_get0_param(sc),
                              X509_V_FLAG_PARTIAL_CHAIN);
  /* both roles: verify the peer's cert, and a client without one fails */
  SSL_CTX_set_verify(sc, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                     NULL);

  X509 *x = SSL_CTX_get0_certificate(sc);
  if (!tls_time(X509_get0_notAfter(x), &tls->not_after)) {
    log_error("peer_tls: peer_tls_cert '%s' has an unreadable expiry date",
              cert);
    goto error;
  }

  char subject[256];
  X509_NAME_oneline(X509_get_subject_name(x), subject, sizeof(subject));
  log_notice(
      "peer_tls: TLS 1.3 between peers with cert '%s' (%s, expires in "
      "%" PRId64 " days) and the CAs in '%s'",
      cert, subject, tls_cert_days_left(tls), ca);

  tls_self_check(tls, cp);
  tls_log_expiry(tls, cp);

  return tls;

error:
  tls_ctx_destroy(tls);
  return NULL;
}

void tls_ctx_destroy(struct tls_ctx *tls) {
  if (tls == NULL) {
    return;
  }
  if (tls->expiry_task != NULL) {
    cancel_task(tls->expiry_task);
  }
  SSL_CTX_free(tls->ssl_ctx);
  dn_free(tls);
}

bool tls_ctx_cert_warned(struct tls_ctx *tls) { return tls->cert_warned; }

/*
 * Whole days until the expiry, rounded down: 0 on the last day, -1 from the
 * moment it expired. Read by the stats thread too: not_after never changes
 * after creation.
 */
int64_t tls_cert_days_left(struct tls_ctx *tls) {
  int64_t left = (int64_t)tls->not_after - (int64_t)time(NULL);

  if (left < 0) {
    return (left - 86399) / 86400;
  }
  return left / 86400;
}

/*
 * A peer with peer_tls off never answers a ClientHello: a node with it on
 * takes it for a dnode message that hasn't fully arrived and waits.
 */
static const char *tls_no_reply_hint(struct tls_conn *tc) {
  if (!tc->is_server && BIO_number_read(SSL_get_rbio(tc->ssl)) == 0) {
    return "; no TLS reply: is peer_tls on there?";
  }
  return "";
}

static void tls_handshake_timeout(void *arg) {
  struct conn *conn = arg;
  struct context *ctx = conn_to_ctx(conn);

  ASSERT(conn->tls != NULL && conn->tls->handshaking);
  conn->tls->hs_timeout = NULL;

  log_warn("%s TLS handshake not finished in %d ms, closing%s", print_obj(conn),
           TLS_HANDSHAKE_TIMEOUT_MS, tls_no_reply_hint(conn->tls));
  stats_pool_incr(ctx, peer_tls_handshake_errors);
  conn->err = ETIMEDOUT;
  core_close_conn(ctx, conn);
}

rstatus_t tls_conn_start(struct context *ctx, struct conn *conn,
                         bool is_server) {
  struct tls_conn *tc;

  ASSERT(ctx->tls != NULL);
  ASSERT(conn->tls == NULL);
  ASSERT(conn->sd >= 0);

  tc = dn_zalloc(sizeof(*tc));
  if (tc == NULL) {
    return DN_ENOMEM;
  }
  tc->wbuf = dn_alloc(TLS_RECORD_MAX);
  if (tc->wbuf == NULL) {
    dn_free(tc);
    return DN_ENOMEM;
  }

  ERR_clear_error();
  tc->ssl = SSL_new(ctx->tls->ssl_ctx);
  if (tc->ssl == NULL || SSL_set_fd(tc->ssl, conn->sd) != 1) {
    log_error("%s TLS setup failed: %s", print_obj(conn), tls_last_error());
    SSL_free(tc->ssl);
    dn_free(tc->wbuf);
    dn_free(tc);
    return DN_ERROR;
  }
  if (is_server) {
    SSL_set_accept_state(tc->ssl);
  } else {
    SSL_set_connect_state(tc->ssl);
  }
  tc->is_server = is_server;
  tc->handshaking = 1;
  tc->hs_timeout =
      schedule_task_1(tls_handshake_timeout, conn, TLS_HANDSHAKE_TIMEOUT_MS);
  conn->tls = tc;

  return DN_OK;
}

bool tls_conn_handshaking(struct conn *conn) {
  return conn->tls != NULL && conn->tls->handshaking;
}

/*
 * Before the first SSL_accept, look at the first byte: a node with peer_tls
 * off sends a dnode message, which gets its own log line and counter
 * instead of an OpenSSL error about a wrong version number.
 */
static rstatus_t tls_peek(struct context *ctx, struct conn *conn) {
  uint8_t first;
  ssize_t n;

  n = recv(conn->sd, &first, 1, MSG_PEEK);
  if (n < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      return DN_EAGAIN;
    }
    conn->err = errno;
    log_error("%s recv failed: %s", print_obj(conn), strerror(errno));
    return DN_ERROR;
  }
  if (n == 0) {
    conn->eof = 1;
    log_debug(LOG_INFO, "%s closed before the TLS handshake", print_obj(conn));
    return DN_ERROR;
  }
  conn->tls->peeked = 1;

  if (first != TLS_HANDSHAKE_RECORD) {
    log_warn("%s rejected: not TLS (first byte 0x%02x); is peer_tls on there?",
             print_obj(conn), first);
    stats_pool_incr(ctx, peer_tls_plaintext_rejected);
    conn->err = EPROTO;
    return DN_ERROR;
  }
  return DN_OK;
}

static void tls_handshake_failed(struct context *ctx, struct conn *conn,
                                 int ssl_err) {
  struct tls_conn *tc = conn->tls;
  const char *reason;
  const char *verify = "";
  const char *hint = tls_no_reply_hint(tc);

  if (ssl_err == SSL_ERROR_SYSCALL && errno != 0) {
    conn->err = errno;
    reason = strerror(errno);
  } else {
    conn->err = EPROTO;
    reason = (ssl_err == SSL_ERROR_ZERO_RETURN ||
              (ssl_err == SSL_ERROR_SYSCALL && ERR_peek_error() == 0))
                 ? "connection closed"
                 : tls_last_error();
  }
  long vr = SSL_get_verify_result(tc->ssl);
  if (vr != X509_V_OK) {
    verify = X509_verify_cert_error_string(vr);
  }
  log_error("%s TLS handshake failed: %s%s%s%s", print_obj(conn), reason,
            *verify ? ", " : "", verify, hint);
  ERR_clear_error();
  stats_pool_incr(ctx, peer_tls_handshake_errors);
  tc->fatal = 1;
}

rstatus_t tls_conn_handshake(struct context *ctx, struct conn *conn) {
  struct tls_conn *tc = conn->tls;
  rstatus_t status;
  int r, ssl_err;

  ASSERT(tc != NULL && tc->handshaking);

  if (tc->is_server && !tc->peeked) {
    status = tls_peek(ctx, conn);
    if (status != DN_OK) {
      return status;
    }
  }

  ERR_clear_error();
  errno = 0;
  r = SSL_do_handshake(tc->ssl);
  if (r == 1) {
    tc->handshaking = 0;
    cancel_task(tc->hs_timeout);
    tc->hs_timeout = NULL;
    stats_pool_incr(ctx, peer_tls_handshakes);
    log_notice("%s TLS handshake done: %s %s", print_obj(conn),
               SSL_get_version(tc->ssl), SSL_get_cipher_name(tc->ssl));
    return DN_OK;
  }

  ssl_err = SSL_get_error(tc->ssl, r);
  switch (ssl_err) {
    case SSL_ERROR_WANT_READ:
      return DN_EAGAIN;
    case SSL_ERROR_WANT_WRITE:
      if (conn_event_add_out(conn) != DN_OK) {
        conn->err = errno;
        return DN_ERROR;
      }
      return DN_EAGAIN;
    default:
      tls_handshake_failed(ctx, conn, ssl_err);
      return DN_ERROR;
  }
}

static void tls_io_failed(struct conn *conn, const char *what, int ssl_err) {
  struct tls_conn *tc = conn->tls;

  if (ssl_err == SSL_ERROR_SYSCALL && errno != 0) {
    conn->err = errno;
    log_error("%s %s failed: %s", print_obj(conn), what, strerror(errno));
  } else if (ssl_err == SSL_ERROR_WANT_READ ||
             ssl_err == SSL_ERROR_WANT_WRITE) {
    conn->err = EPROTO;
    log_error("%s %s wants to %s after the handshake, closing", print_obj(conn),
              what, ssl_err == SSL_ERROR_WANT_READ ? "read" : "write");
    return;
  } else {
    conn->err = EPROTO;
    log_error("%s %s failed: %s", print_obj(conn), what, tls_last_error());
  }
  ERR_clear_error();
  tc->fatal = 1;
}

ssize_t tls_conn_recv(struct conn *conn, void *buf, size_t size) {
  struct tls_conn *tc = conn->tls;
  size_t n = 0;
  int r, ssl_err;

  ASSERT(tc != NULL && !tc->handshaking);
  ASSERT(buf != NULL);
  ASSERT(size > 0);
  ASSERT(conn->recv_ready);

  ERR_clear_error();
  errno = 0;
  r = SSL_read_ex(tc->ssl, buf, size, &n);

  log_debug(LOG_VERB, "%s TLS recv %zd of %zu", print_obj(conn),
            r == 1 ? (ssize_t)n : (ssize_t)-1, size);

  if (r == 1) {
    /* a short read is not a drained socket here: keep reading */
    conn->recv_bytes += n;
    return (ssize_t)n;
  }

  conn->recv_ready = 0;
  ssl_err = SSL_get_error(tc->ssl, r);
  switch (ssl_err) {
    case SSL_ERROR_WANT_READ:
      log_debug(LOG_VERB, "%s TLS recv not ready - want read", print_obj(conn));
      return DN_EAGAIN;
    case SSL_ERROR_ZERO_RETURN:
      conn->eof = 1;
      log_debug(LOG_NOTICE, "%s TLS recv eof rb %zu sb %zu", print_obj(conn),
                conn->recv_bytes, conn->send_bytes);
      return 0;
    default:
      tls_io_failed(conn, "TLS recv", ssl_err);
      return DN_ERROR;
  }
}

/*
 * Copies up to one record's worth of the iovecs from (idx, off) into wbuf;
 * advances (idx, off) past the copied bytes.
 */
static size_t tls_stage(struct tls_conn *tc, struct array *sendv, uint32_t *idx,
                        size_t *off) {
  size_t len = 0;

  while (len < TLS_RECORD_MAX && *idx < array_n(sendv)) {
    struct iovec *iov = array_get(sendv, *idx);
    size_t take = iov->iov_len - *off;

    if (take > TLS_RECORD_MAX - len) {
      take = TLS_RECORD_MAX - len;
    }
    memcpy(tc->wbuf + len, (uint8_t *)iov->iov_base + *off, take);
    len += take;
    *off += take;
    if (*off == iov->iov_len) {
      (*idx)++;
      *off = 0;
    }
  }
  return len;
}

/* moves (idx, off) forward by skip bytes */
static void tls_skip(struct array *sendv, uint32_t *idx, size_t *off,
                     size_t skip) {
  while (skip > 0) {
    struct iovec *iov = array_get(sendv, *idx);
    size_t left = iov->iov_len - *off;

    if (skip < left) {
      *off += skip;
      return;
    }
    skip -= left;
    (*idx)++;
    *off = 0;
  }
}

ssize_t tls_conn_sendv(struct conn *conn, struct array *sendv, size_t nsend) {
  struct tls_conn *tc = conn->tls;
  size_t total = 0, len, n;
  uint32_t idx = 0;
  size_t off = 0;
  int r, ssl_err;

  ASSERT(tc != NULL && !tc->handshaking);
  ASSERT(array_n(sendv) > 0);
  ASSERT(nsend != 0);
  ASSERT(conn->send_ready);

  if (tc->wpending != 0) {
    /* the record OpenSSL still holds; the iovecs start with the same bytes */
    ASSERT(nsend >= tc->wpending);
    ERR_clear_error();
    errno = 0;
    r = SSL_write_ex(tc->ssl, tc->wbuf, tc->wpending, &n);
    if (r != 1) {
      goto blocked;
    }
    total = tc->wpending;
    tc->wpending = 0;
    tls_skip(sendv, &idx, &off, total);
  }

  while (total < nsend) {
    len = tls_stage(tc, sendv, &idx, &off);
    ASSERT(len > 0);
    ERR_clear_error();
    errno = 0;
    r = SSL_write_ex(tc->ssl, tc->wbuf, len, &n);
    if (r != 1) {
      tc->wpending = len;
      goto blocked;
    }
    total += len;
  }

  log_debug(LOG_VERB, "%s TLS sendv %zu of %zu in %" PRIu32 " buffers",
            print_obj(conn), total, nsend, array_n(sendv));
  conn->send_bytes += total;
  return (ssize_t)total;

blocked:
  conn->send_ready = 0;
  ssl_err = SSL_get_error(tc->ssl, r);
  if (ssl_err == SSL_ERROR_WANT_WRITE) {
    log_debug(LOG_VERB, "%s TLS sendv %zu of %zu, then not ready - want write",
              print_obj(conn), total, nsend);
    conn->send_bytes += total;
    return total > 0 ? (ssize_t)total : DN_EAGAIN;
  }
  tls_io_failed(conn, "TLS sendv", ssl_err);
  return DN_ERROR;
}

void tls_conn_close(struct conn *conn) {
  struct tls_conn *tc = conn->tls;

  if (tc == NULL) {
    return;
  }
  if (tc->hs_timeout != NULL) {
    cancel_task(tc->hs_timeout);
  }
  if (!tc->handshaking && !tc->fatal) {
    /* best effort close_notify; the socket is non-blocking */
    ERR_clear_error();
    SSL_shutdown(tc->ssl);
    ERR_clear_error();
  }
  SSL_free(tc->ssl);
  dn_free(tc->wbuf);
  dn_free(tc);
  conn->tls = NULL;
}

/*
 * For the unit test: a handshake between two contexts over an in-memory
 * BIO pair, no sockets. A NULL client stands for a bare TLS 1.3 client
 * without a cert and without verification, to check that the server
 * insists on a client cert. *detail gets the negotiated version on
 * success and the reason on failure.
 */
rstatus_t tls_test_handshake(struct tls_ctx *client, struct tls_ctx *server,
                             const char **detail) {
  SSL_CTX *bare = NULL;
  SSL *c = NULL, *s = NULL;
  BIO *cb = NULL, *sb = NULL;
  rstatus_t status = DN_ERROR;
  bool cdone = false, sdone = false;

  *detail = "setup failed";
  ERR_clear_error();

  if (client == NULL) {
    bare = SSL_CTX_new(TLS_client_method());
    if (bare == NULL ||
        SSL_CTX_set_min_proto_version(bare, TLS1_3_VERSION) != 1) {
      goto out;
    }
  }
  c = SSL_new(client != NULL ? client->ssl_ctx : bare);
  s = SSL_new(server->ssl_ctx);
  if (c == NULL || s == NULL || BIO_new_bio_pair(&cb, 0, &sb, 0) != 1) {
    goto out;
  }
  SSL_set_bio(c, cb, cb);
  SSL_set_bio(s, sb, sb);
  cb = sb = NULL; /* owned by the SSL objects now */
  SSL_set_connect_state(c);
  SSL_set_accept_state(s);

  /* each side runs until it needs the other's output; a few rounds do */
  for (int round = 0; round < 10 && !(cdone && sdone); round++) {
    if (!cdone) {
      int r = SSL_do_handshake(c);
      if (r == 1) {
        cdone = true;
      } else {
        int e = SSL_get_error(c, r);
        if (e != SSL_ERROR_WANT_READ && e != SSL_ERROR_WANT_WRITE) {
          *detail = tls_last_error();
          goto out;
        }
      }
    }
    if (!sdone) {
      int r = SSL_do_handshake(s);
      if (r == 1) {
        sdone = true;
      } else {
        int e = SSL_get_error(s, r);
        if (e != SSL_ERROR_WANT_READ && e != SSL_ERROR_WANT_WRITE) {
          *detail = tls_last_error();
          goto out;
        }
      }
    }
  }
  if (!(cdone && sdone)) {
    *detail = "handshake did not finish";
    goto out;
  }

  /* and a message through: the server may still have to read Finished */
  size_t n;
  char buf[4];
  if (SSL_write_ex(c, "ping", 4, &n) != 1 || n != 4 ||
      SSL_read_ex(s, buf, sizeof(buf), &n) != 1 || n != 4 ||
      memcmp(buf, "ping", 4) != 0) {
    *detail = "data did not get through";
    goto out;
  }

  *detail = SSL_get_version(c);
  status = DN_OK;

out:
  BIO_free(cb);
  BIO_free(sb);
  SSL_free(c);
  SSL_free(s);
  SSL_CTX_free(bare);
  ERR_clear_error();
  return status;
}
