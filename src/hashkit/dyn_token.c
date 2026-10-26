/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "dyn_token.h"
#include <stddef.h>
#include <stdint.h>
#include "../dyn_array.h"
#include "../dyn_log.h"
#include "../dyn_string.h"
#include "../dyn_util.h"

void init_dyn_token(struct dyn_token *token) {
  token->signum = 0;
  token->len = 0;
}

void deinit_dyn_token(struct dyn_token *token) {
  token->signum = 0;
  token->len = 0;
}

// This implementation does not take into account that token's val can be very
// large, larger than an uint32 number
rstatus_t size_dyn_token(struct dyn_token *token, uint32_t token_len) {
  ASSERT(token_len <= 4);
  token->len = token_len;
  token->signum = 0;

  return DN_OK;
}

// This implementation does not take into account that token's val can be very
// large, larger than an uint32 number
rstatus_t copy_dyn_token(const struct dyn_token *src, struct dyn_token *dst) {
  rstatus_t status = size_dyn_token(dst, 1);
  set_int_dyn_token(dst, src->mag[0]);
  return status;
}

// This implementation does not take into account that token's val can be very
// large, larger than an uint32 number
void set_int_dyn_token(struct dyn_token *token, uint32_t val) {
  token->mag[0] = val;
  token->len = 1;
  token->signum = val > 0 ? 1 : 0;
}

void print_dyn_token(struct dyn_token *token, int num_tabs) {
  if (token == NULL) log_debug(LOG_VERB, "Token is null!!!!!");

  if (num_tabs < 0) num_tabs = 0;

  log_debug(LOG_VERB, "%*cToken : %" PRIu32 " %" PRIu32 " %" PRIu32 " ",
            num_tabs, '\t', token->signum, token->mag[0], token->len);
}

/*
 * Tokens are 32-bit: only decimal digits, from 0 to UINT32_MAX, are accepted.
 * Anything else (a sign, spaces, a bigger number) is DN_ERROR and leaves the
 * token 0.
 */
rstatus_t parse_dyn_token(uint8_t *start, uint32_t len,
                          struct dyn_token *token) {
  ASSERT(token != NULL);

  set_int_dyn_token(token, 0);
  if (len == 0) {
    return DN_ERROR;
  }

  uint64_t val = 0;
  for (uint32_t i = 0; i < len; i++) {
    if (start[i] < '0' || start[i] > '9') {
      return DN_ERROR;
    }
    val = val * 10 + (uint64_t)(start[i] - '0');
    if (val > UINT32_MAX) {
      return DN_ERROR;
    }
  }

  set_int_dyn_token(token, (uint32_t)val);
  return DN_OK;
}

int32_t cmp_dyn_token(const struct dyn_token *t1, const struct dyn_token *t2) {
  ASSERT(t1 != NULL);
  ASSERT(t2 != NULL);

  if (t1->signum == t2->signum) {
    if (t1->signum == 0) {
      return 0;
    }

    if (t1->len == t2->len) {
      uint32_t i;
      for (i = 0; i < t1->len; i++) {
        uint32_t a = t1->mag[i];
        uint32_t b = t2->mag[i];
        if (a != b) {
          return a > b ? 1 : -1;
        }
      }
      return 0;
    }

    return t1->len > t2->len ? 1 : -1;
  }

  return t1->signum > t2->signum ? 1 : -1;
}

/*
 * Does the work of reading an array of chars, and constructing the tokens
 * for the array.
 */
rstatus_t derive_tokens(struct array *tokens, uint8_t *start, uint8_t *end) {
  ASSERT(end > start);
  uint8_t *p = end;
  uint8_t *q;
  while (p >= start) {
    struct dyn_token *token = array_push(tokens);
    ASSERT(token != NULL);
    init_dyn_token(token);

    q = dn_strrchr(p, start, ',');
    if (q == NULL) {
      q = start; /* we're at the beginning of the list */
    } else {
      q++;
    }

    uint32_t len = 0;
    if (p == end) {
      len = (uint32_t)(p - q);
    } else {
      len = (uint32_t)(p - q + 1);
    }

    rstatus_t status = parse_dyn_token(q, len, token);
    if (status != DN_OK) {
      log_error("invalid token '%.*s': must be a number from 0 to %" PRIu32,
                (int)len, q, UINT32_MAX);
      return DN_ERROR;
    }

    p = q - 2;
  }

  return DN_OK;
}

rstatus_t derive_token(struct dyn_token *token, uint8_t *start, uint8_t *end) {
  ASSERT(end > start);
  uint8_t *p = end;
  uint8_t *q;
  ASSERT(token != NULL);
  init_dyn_token(token);

  if (p >= start) {
    q = dn_strrchr(p, start, ',');
    if (q == NULL) {
      q = start; /* we're at the beginning of the list */
    } else {
      q++;
    }

    uint32_t len = 0;
    if (p == end) {
      len = (uint32_t)(p - q);
    } else {
      len = (uint32_t)(p - q + 1);
    }

    rstatus_t status = parse_dyn_token(q, len, token);
    if (status != DN_OK) {
      log_error("invalid token '%.*s': must be a number from 0 to %" PRIu32,
                (int)len, q, UINT32_MAX);
      return DN_ERROR;
    }

    p = q - 2;
  }

  return DN_OK;
}
