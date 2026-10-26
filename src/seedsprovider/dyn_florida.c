#include <arpa/inet.h>
#include <ctype.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>

#include "dyn_core.h"
#include "dyn_seeds_provider.h"
#include "dyn_string.h"

/***************************************************************************
 * Keep polling the local REST service at the address
 * http://127.0.0.1:8080/REST/v1/admin/get_seeds
 * Expect response is a list of peers in the format:
 *   peer_host1:peer_listen_port:rack:dc:peer_token1|peer_host2:peer_listen_port:rack:dc:peer_token2|...
 * For example:
 *   ec2-54-145-17-101.compute-1.amazonaws.com:8101:dyno_pds--useast1e:us-east-1:1383429731|ec2-54-101-51-17.eu-west-1.compute.amazonaws.com:8101:dyno_pds--euwest1c:eu-west-1:1383429731
 ****************************************************************************/

#ifndef FLORIDA_IP
#define FLORIDA_IP "127.0.0.1"
#endif

#ifndef FLORIDA_PORT
#define FLORIDA_PORT 8080
#endif

#ifndef FLORIDA_REQUEST
#define FLORIDA_REQUEST                                                      \
  "GET /REST/v1/admin/get_seeds HTTP/1.0\r\nHost: 127.0.0.1\r\nUser-Agent: " \
  "HTMLGET 1.0\r\n\r\n";
#endif

/* Bounds connect(), each send() and each recv(), so a hung Florida can't stall
 * the gossip thread. */
#define FLORIDA_TIMEOUT_SEC 5

/* Room for the status line and headers, on top of the seeds themselves. */
#define FLORIDA_HEADER_MAX 8192

static char *floridaIp = NULL;
static int floridaPort = 0;
static char *request = NULL;
static int isOsVarEval = 0;

static void evalOSVar();

static int64_t last = 0;  // storing last time for seeds check
static uint32_t last_seeds_hash = 0;

static void evalOSVar() {
  if (isOsVarEval == 0) {
    request = (getenv("DYNOMITE_FLORIDA_REQUEST") != NULL)
                  ? getenv("DYNOMITE_FLORIDA_REQUEST")
                  : FLORIDA_REQUEST;
    floridaPort = (getenv("DYNOMITE_FLORIDA_PORT") != NULL)
                      ? atoi(getenv("DYNOMITE_FLORIDA_PORT"))
                      : FLORIDA_PORT;
    floridaIp = (getenv("DYNOMITE_FLORIDA_IP") != NULL)
                    ? getenv("DYNOMITE_FLORIDA_IP")
                    : FLORIDA_IP;
    isOsVarEval = 1;
  }
}

static bool seeds_check() {
  msec_t now = dn_msec_now();

  int64_t delta = (int64_t)(now - last);
  log_debug(LOG_VERB, "Delta or elapsed time : %lu", delta);
  log_debug(LOG_VERB, "Seeds check internal %d", SEEDS_CHECK_INTERVAL);

  if (delta > SEEDS_CHECK_INTERVAL) {
    last = now;
    return true;
  }

  return false;
}

static uint32_t hash_seeds(uint8_t *seeds, size_t length) {
  const uint8_t *ptr = seeds;
  uint32_t value = 0;

  while (length--) {
    uint32_t val = (uint32_t)*ptr++;
    value += val;
    value += (value << 10);
    value ^= (value >> 6);
  }
  value += (value << 3);
  value ^= (value >> 11);
  value += (value << 15);

  return value;
}

/* strerror(), except that a socket timeout reads as such. */
static const char *florida_strerror(int err) {
  /* connect() fails with EINPROGRESS on a timeout, send() and recv() with
   * EAGAIN or EWOULDBLOCK. */
  if (err == EINPROGRESS || err == EAGAIN || err == EWOULDBLOCK) {
    return "timed out";
  }
  return strerror(err);
}

/* Connect to Florida, with timeouts; returns the socket or -1. */
static int florida_connect(void) {
  struct sockaddr_in remote;
  struct timeval timeout = {FLORIDA_TIMEOUT_SEC, 0};
  int sock;

  memset(&remote, 0, sizeof(remote));
  remote.sin_family = AF_INET;
  remote.sin_port = htons((uint16_t)floridaPort);
  if (inet_pton(AF_INET, floridaIp, &remote.sin_addr) != 1) {
    log_error("Invalid Florida address '%s'", floridaIp);
    return -1;
  }

  sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock < 0) {
    log_error("Unable to create a socket for Florida: %s", strerror(errno));
    return -1;
  }

  /* On Linux, SO_SNDTIMEO also bounds connect(). */
  int status = setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                          sizeof(timeout));
  if (status == 0) {
    status = setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                        sizeof(timeout));
  }
  if (status == 0) {
    status = connect(sock, (struct sockaddr *)&remote, sizeof(remote));
  }
  if (status < 0) {
    log_debug(LOG_VVERB, "Unable to connect to Florida at %s:%d: %s",
              floridaIp, floridaPort, florida_strerror(errno));
    close(sock);
    return -1;
  }

  return sock;
}

/*
 * Send the request and read the whole response into rsp, which has room for
 * size bytes. Returns the response length, or -1 on an error, a timeout or a
 * response larger than size.
 */
static ssize_t florida_query(int sock, uint8_t *rsp, size_t size) {
  size_t len = dn_strlen(request), sent = 0, rcvd = 0;
  ssize_t n;

  while (sent < len) {
    n = send(sock, request + sent, len - sent, 0);
    if (n < 0) {
      log_error("Unable to send the query to Florida: %s",
                florida_strerror(errno));
      return -1;
    }
    sent += (size_t)n;
  }

  /* The request is HTTP/1.0, so Florida closes the connection at the end. */
  for (;;) {
    if (rcvd == size) {
      log_error("Florida response is larger than %zu bytes", size);
      return -1;
    }
    n = recv(sock, rsp + rcvd, size - rcvd, 0);
    if (n == 0) {
      return (ssize_t)rcvd;
    }
    if (n < 0) {
      log_error("Unable to read the Florida response: %s",
                florida_strerror(errno));
      return -1;
    }
    rcvd += (size_t)n;
  }
}

/* Is it a 200 response: "HTTP/1.x 200", then a space or the end of the line? */
static bool florida_status_ok(const uint8_t *rsp, size_t len) {
  return len >= 13 && memcmp(rsp, "HTTP/1.", 7) == 0 && isdigit(rsp[7]) &&
         memcmp(rsp + 8, " 200", 4) == 0 && (rsp[12] == ' ' || rsp[12] == CR);
}

/* Find the body, after the empty line ending the headers; NULL if none. */
static const uint8_t *florida_body(const uint8_t *rsp, size_t len) {
  for (size_t i = 0; i + 4 <= len; i++) {
    if (memcmp(rsp + i, CRLF CRLF, 4) == 0) {
      return rsp + i + 4;
    }
  }
  return NULL;
}

uint8_t florida_get_seeds(struct context *ctx, struct mbuf *seeds_buf) {
  evalOSVar();

  log_debug(LOG_VVERB, "Running florida_get_seeds!");

  if (!seeds_check()) {
    return DN_NOOPS;
  }

  int sock = florida_connect();
  if (sock < 0) {
    return DN_ERROR;
  }

  mbuf_rewind(seeds_buf);

  size_t size = FLORIDA_HEADER_MAX + mbuf_remaining_space(seeds_buf);
  uint8_t *rsp = dn_alloc(size);
  if (rsp == NULL) {
    close(sock);
    return DN_ERROR;
  }

  ssize_t len = florida_query(sock, rsp, size);
  close(sock);
  if (len < 0) {
    dn_free(rsp);
    return DN_ERROR;
  }

  if (!florida_status_ok(rsp, (size_t)len)) {
    log_error("Received Error from Florida while getting seeds");
    loga_hexdump(rsp, MIN((size_t)len, 256),
                 "Florida Response with %zd bytes of data", len);
    dn_free(rsp);
    return DN_ERROR;
  }

  const uint8_t *body = florida_body(rsp, (size_t)len);
  size_t body_len = body == NULL ? 0 : (size_t)(rsp + len - body);
  if (body_len > mbuf_remaining_space(seeds_buf)) {
    log_error("Florida seeds are larger than %" PRIu32 " bytes",
              mbuf_remaining_space(seeds_buf));
    dn_free(rsp);
    return DN_ERROR;
  }
  mbuf_copy(seeds_buf, body, body_len);
  dn_free(rsp);

  if (mbuf_length(seeds_buf) == 0) {
    log_error("No seeds were found in Florida response");
    return DN_ERROR;
  }

  uint32_t seeds_hash = hash_seeds(seeds_buf->pos, mbuf_length(seeds_buf));

  if (last_seeds_hash != seeds_hash) {
    last_seeds_hash = seeds_hash;
  } else {
    return DN_NOOPS;
  }

  return DN_OK;
}
