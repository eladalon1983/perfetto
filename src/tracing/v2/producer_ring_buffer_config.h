/*
 * Copyright (C) 2026 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_TRACING_V2_PRODUCER_RING_BUFFER_CONFIG_H_
#define SRC_TRACING_V2_PRODUCER_RING_BUFFER_CONFIG_H_

#include <stddef.h>
#include <stdint.h>

#include <optional>

#include "perfetto/tracing/core/forward_decls.h"

namespace perfetto::tracing_v2 {

// The resolved settings used to allocate a ring buffer and create its arbiter.
struct ProducerRingBufferConfig {
  // Mapping size, including the ring buffer header.
  size_t shmem_size_bytes = 0;
  uint32_t chunk_size = 0;
  // Number of outstanding positions at which a publication requests a drain.
  uint32_t drain_occupancy_threshold = 0;
};

// Resolves the experimental settings of the first instance that selects v2.
// The endpoint calls this once, before allocation, and keeps the selected
// settings for the connection.
// - Chooses a chunk size by weight. An absent weight counts as 1, and 0 means
//   never. Invalid sizes are skipped because older services may not reject
//   them. Uses kMinChunkSize if no option can be chosen.
// - Sizes the mapping with RingBufferSizeForShmSizeHint(). The hint counts
//   chunk bytes, while the maximum and returned size include the header.
// - Computes the drain threshold from the chosen layout and drain percentage.
//   The percentage must be -1 to 100, as validated by the service.
// Returns nullopt if no valid ring buffer fits the size limits.
std::optional<ProducerRingBufferConfig> ResolveProducerRingBufferConfig(
    const DataSourceConfig& config,
    size_t shmem_size_hint_bytes,
    size_t max_shmem_size_bytes);

}  // namespace perfetto::tracing_v2

#endif  // SRC_TRACING_V2_PRODUCER_RING_BUFFER_CONFIG_H_
