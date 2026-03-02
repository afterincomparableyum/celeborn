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

#include "celeborn/protocol/Encoders.h"

using namespace celeborn;
using namespace celeborn::protocol;

TEST(EncodersArrayTest, stringArrayEncodedLength) {
  std::vector<std::string> arr = {"hello", "world"};
  // 4 (count) + (4 + 5) + (4 + 5) = 22
  EXPECT_EQ(encodedLength(arr), 22);
}

TEST(EncodersArrayTest, stringArrayEncodedLengthEmpty) {
  std::vector<std::string> arr;
  // 4 (count)
  EXPECT_EQ(encodedLength(arr), 4);
}

TEST(EncodersArrayTest, stringArrayEncodeAndDecode) {
  std::vector<std::string> arr = {"foo", "bar", "baz"};
  int len = encodedLength(arr);
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(len);
  encode(*writeBuffer, arr);
  auto readBuffer = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));

  int count = readBuffer->read<int>();
  EXPECT_EQ(count, 3);
  EXPECT_EQ(decode(*readBuffer), "foo");
  EXPECT_EQ(decode(*readBuffer), "bar");
  EXPECT_EQ(decode(*readBuffer), "baz");
}

TEST(EncodersArrayTest, intArrayEncodedLength) {
  std::vector<int> arr = {1, 2, 3};
  // 4 (count) + 4 * 3 = 16
  EXPECT_EQ(encodedLength(arr), 16);
}

TEST(EncodersArrayTest, intArrayEncodedLengthEmpty) {
  std::vector<int> arr;
  EXPECT_EQ(encodedLength(arr), 4);
}

TEST(EncodersArrayTest, intArrayEncodeAndDecode) {
  std::vector<int> arr = {10, 20, 30};
  int len = encodedLength(arr);
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(len);
  encode(*writeBuffer, arr);
  auto readBuffer = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));

  int count = readBuffer->read<int>();
  EXPECT_EQ(count, 3);
  EXPECT_EQ(readBuffer->read<int>(), 10);
  EXPECT_EQ(readBuffer->read<int>(), 20);
  EXPECT_EQ(readBuffer->read<int>(), 30);
}

TEST(EncodersArrayTest, stringArrayWithEmptyStrings) {
  std::vector<std::string> arr = {"", "nonempty", ""};
  int len = encodedLength(arr);
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(len);
  encode(*writeBuffer, arr);
  auto readBuffer = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));

  int count = readBuffer->read<int>();
  EXPECT_EQ(count, 3);
  EXPECT_EQ(decode(*readBuffer), "");
  EXPECT_EQ(decode(*readBuffer), "nonempty");
  EXPECT_EQ(decode(*readBuffer), "");
}
