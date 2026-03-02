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

#include "celeborn/network/Message.h"
#include "celeborn/protocol/Encoders.h"

using namespace celeborn;
using namespace celeborn::network;

TEST(PushMergedDataMessageTest, encodePushMergedData) {
  const std::string body = "test-body-data";
  auto bodyBuffer = memory::ByteBuffer::createWriteOnly(body.size());
  bodyBuffer->writeFromString(body);
  const long requestId = 2000;
  const uint8_t mode = 0;
  const std::string shuffleKey = "test-shuffle-key";
  std::vector<std::string> partitionUniqueIds = {"pid-0", "pid-1"};
  std::vector<int> batchOffsets = {0, 7};

  auto pushMergedData = std::make_unique<PushMergedData>(
      requestId,
      mode,
      shuffleKey,
      partitionUniqueIds,
      batchOffsets,
      memory::ByteBuffer::toReadOnly(std::move(bodyBuffer)));

  auto encodedBuffer = pushMergedData->encode();

  // Read header: encodedLength, type, bodyLength
  int encodedLength = encodedBuffer->read<int32_t>();
  EXPECT_EQ(encodedBuffer->read<uint8_t>(), Message::Type::PUSH_MERGED_DATA);
  EXPECT_EQ(encodedBuffer->read<int32_t>(), body.size());

  // Read internal encoded fields
  EXPECT_EQ(encodedBuffer->read<long>(), requestId);
  EXPECT_EQ(encodedBuffer->read<uint8_t>(), mode);

  // shuffleKey
  int shuffleKeyLen = encodedBuffer->read<int32_t>();
  EXPECT_EQ(shuffleKeyLen, shuffleKey.size());
  EXPECT_EQ(encodedBuffer->readToString(shuffleKeyLen), shuffleKey);

  // partitionUniqueIds
  int numPartitionIds = encodedBuffer->read<int32_t>();
  EXPECT_EQ(numPartitionIds, 2);
  int len0 = encodedBuffer->read<int32_t>();
  EXPECT_EQ(encodedBuffer->readToString(len0), "pid-0");
  int len1 = encodedBuffer->read<int32_t>();
  EXPECT_EQ(encodedBuffer->readToString(len1), "pid-1");

  // batchOffsets
  int numOffsets = encodedBuffer->read<int32_t>();
  EXPECT_EQ(numOffsets, 2);
  EXPECT_EQ(encodedBuffer->read<int32_t>(), 0);
  EXPECT_EQ(encodedBuffer->read<int32_t>(), 7);

  // body
  EXPECT_EQ(encodedBuffer->readToString(body.size()), body);
}

TEST(PushMergedDataMessageTest, copyConstructor) {
  const std::string body = "copy-body";
  auto bodyBuffer = memory::ByteBuffer::createWriteOnly(body.size());
  bodyBuffer->writeFromString(body);

  PushMergedData original(
      3000,
      1,
      "shuffle-key",
      {"p1", "p2"},
      {0, 10},
      memory::ByteBuffer::toReadOnly(std::move(bodyBuffer)));

  PushMergedData copy(original);
  EXPECT_EQ(copy.requestId(), original.requestId());
  EXPECT_EQ(copy.mode(), original.mode());
  EXPECT_EQ(copy.shuffleKey(), original.shuffleKey());
  EXPECT_EQ(copy.partitionUniqueIds().size(), 2);
  EXPECT_EQ(copy.batchOffsets().size(), 2);
}

TEST(PushMergedDataMessageTest, encodedLengthIsCorrect) {
  const std::string shuffleKey = "sk";
  std::vector<std::string> partitionUniqueIds = {"a", "bb"};
  std::vector<int> batchOffsets = {0, 5};

  auto bodyBuffer = memory::ByteBuffer::createWriteOnly(3);
  bodyBuffer->writeFromString("xyz");

  PushMergedData msg(
      1,
      0,
      shuffleKey,
      partitionUniqueIds,
      batchOffsets,
      memory::ByteBuffer::toReadOnly(std::move(bodyBuffer)));

  auto encoded = msg.encode();
  int reportedEncodedLength = encoded->read<int32_t>();

  // Verify the reported encoded length matches the actual encoded content
  int expectedEncodedLength =
      sizeof(long) + sizeof(uint8_t) +
      protocol::encodedLength(shuffleKey) +
      protocol::encodedLength(partitionUniqueIds) +
      protocol::encodedLength(batchOffsets);
  EXPECT_EQ(reportedEncodedLength, expectedEncodedLength);
}
