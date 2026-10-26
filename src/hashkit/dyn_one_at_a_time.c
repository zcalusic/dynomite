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

/*
 * HashKit
 * Copyright (C) 2009 Brian Aker
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */

/*
 * This has is Jenkin's "One at A time Hash".
 * http://en.wikipedia.org/wiki/Jenkins_hash_function
 */

#include <dyn_core.h>
#include <dyn_token.h>

rstatus_t hash_one_at_a_time(const unsigned char *key, size_t key_length,
                             struct dyn_token *token) {
  /*
   * Bytes are added as signed chars, as they always were on x86, whose char is
   * signed. Where char is unsigned, as on arm64, keys with bytes from 0x80 up
   * used to hash differently; this keeps the x86 values, so existing keys stay
   * where they are.
   */
  const signed char *ptr = (const signed char *)key;
  uint32_t value = 0;

  while (key_length--) {
    uint32_t val = (uint32_t)*ptr++;
    value += val;
    value += (value << 10);
    value ^= (value >> 6);
  }
  value += (value << 3);
  value ^= (value >> 11);
  value += (value << 15);

  size_dyn_token(token, 1);
  set_int_dyn_token(token, value);

  return DN_OK;
}
