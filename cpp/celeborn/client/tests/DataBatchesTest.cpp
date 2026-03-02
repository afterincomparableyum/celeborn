/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include "celeborn/client/writer/PushState.h"

using namespace celeborn;
using namespace celeborn::client;

namespace {
std::unique_ptr<memory::ReadOnlyByteBuffer> createBody(size_t size) {
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(size);
  for (size_t i = 0; i < size; i++) {
    writeBuffer->write<uint8_t>(static_cast<uint8_t>(i % 256));
  }
  return memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
}

std::shared_ptr<const protocol::PartitionLocation> createLocation(int id) {
  auto loc = std::make_shared<protocol::PartitionLocation>();
  loc->id = id;
  loc->epoch = 0;
  loc->host = "localhost";
  loc->pushPort = 9000 + id;
  return loc;
}
} // namespace

TEST(DataBatchesTest, addAndGetTotalSize) {
  DataBatches batches;
  EXPECT_EQ(batches.getTotalSize(), 0);

  batches.addDataBatch(createLocation(0), 1, createBody(10));
  EXPECT_EQ(batches.getTotalSize(), 10);

  batches.addDataBatch(createLocation(1), 2, createBody(20));
  EXPECT_EQ(batches.getTotalSize(), 30);
}

TEST(DataBatchesTest, requireBatchesAll) {
  DataBatches batches;
  batches.addDataBatch(createLocation(0), 1, createBody(10));
  batches.addDataBatch(createLocation(1), 2, createBody(20));

  auto result = batches.requireBatches();
  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(batches.getTotalSize(), 0);
}

TEST(DataBatchesTest, requireBatchesWithRequestSize) {
  DataBatches batches;
  batches.addDataBatch(createLocation(0), 1, createBody(10));
  batches.addDataBatch(createLocation(1), 2, createBody(20));
  batches.addDataBatch(createLocation(2), 3, createBody(15));

  // Request size 15: should get first batch (10 bytes), then since 10 < 15
  // continue and get second batch (20 bytes), total 30 now >= 15, stop.
  auto result = batches.requireBatches(15);
  EXPECT_EQ(result.size(), 2);
  // Remaining should be the third batch.
  EXPECT_EQ(batches.getTotalSize(), 15);
}

TEST(DataBatchesTest, requireBatchesWithLargeRequestSize) {
  DataBatches batches;
  batches.addDataBatch(createLocation(0), 1, createBody(10));
  batches.addDataBatch(createLocation(1), 2, createBody(20));

  auto result = batches.requireBatches(100);
  EXPECT_EQ(result.size(), 2);
  EXPECT_EQ(batches.getTotalSize(), 0);
}

TEST(DataBatchesTest, requireBatchesPreservesBatchData) {
  DataBatches batches;
  auto loc = createLocation(42);
  batches.addDataBatch(loc, 7, createBody(5));

  auto result = batches.requireBatches();
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0].loc->id, 42);
  EXPECT_EQ(result[0].batchId, 7);
  EXPECT_EQ(result[0].body->remainingSize(), 5);
}

TEST(PushStateDataBatchesTest, addBatchDataBelowThreshold) {
  const auto conf = conf::CelebornConf();
  PushState pushState(conf);

  PushState::AddressPair addr("host1:9000", "host2:9000");
  auto loc = createLocation(0);

  bool exceeds = pushState.addBatchData(
      addr, loc, 1, createBody(10), 100 /* pushBufferMaxSize */);
  EXPECT_FALSE(exceeds);
}

TEST(PushStateDataBatchesTest, addBatchDataExceedsThreshold) {
  const auto conf = conf::CelebornConf();
  PushState pushState(conf);

  PushState::AddressPair addr("host1:9000", "host2:9000");
  auto loc = createLocation(0);

  pushState.addBatchData(addr, loc, 1, createBody(50), 100);
  bool exceeds = pushState.addBatchData(
      addr, loc, 2, createBody(60), 100);
  EXPECT_TRUE(exceeds);
}

TEST(PushStateDataBatchesTest, takeDataBatches) {
  const auto conf = conf::CelebornConf();
  PushState pushState(conf);

  PushState::AddressPair addr("host1:9000", "host2:9000");
  auto loc = createLocation(0);

  pushState.addBatchData(addr, loc, 1, createBody(10), 100);

  auto taken = pushState.takeDataBatches(addr);
  ASSERT_NE(taken, nullptr);
  EXPECT_EQ(taken->getTotalSize(), 10);

  // Second take should return nullptr.
  auto takenAgain = pushState.takeDataBatches(addr);
  EXPECT_EQ(takenAgain, nullptr);
}

TEST(PushStateDataBatchesTest, forEachBatchEntry) {
  const auto conf = conf::CelebornConf();
  PushState pushState(conf);

  PushState::AddressPair addr1("host1:9000", "");
  PushState::AddressPair addr2("host2:9000", "");
  auto loc1 = createLocation(0);
  auto loc2 = createLocation(1);

  pushState.addBatchData(addr1, loc1, 1, createBody(10), 100);
  pushState.addBatchData(addr2, loc2, 2, createBody(20), 100);

  int count = 0;
  pushState.forEachBatchEntry(
      [&](const PushState::AddressPair&, std::shared_ptr<DataBatches>) {
        count++;
      });
  EXPECT_EQ(count, 2);
}
