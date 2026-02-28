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

#include <roaring/roaring.hh>

#include "celeborn/proto/TransportMessagesCpp.pb.h"
#include "celeborn/protocol/PartitionLocation.h"

using namespace celeborn::protocol;

std::unique_ptr<PbStorageInfo> generateStorageInfoPb() {
  auto pbStorageInfo = std::make_unique<PbStorageInfo>();
  pbStorageInfo->set_type(1);
  pbStorageInfo->set_mountpoint("test_mountpoint");
  pbStorageInfo->set_finalresult(true);
  pbStorageInfo->set_filepath("test_filepath");
  pbStorageInfo->set_availablestoragetypes(1);
  return std::move(pbStorageInfo);
}

void verifyStorageInfo(const StorageInfo* storageInfo) {
  EXPECT_EQ(storageInfo->type, 1);
  EXPECT_EQ(storageInfo->mountPoint, "test_mountpoint");
  EXPECT_EQ(storageInfo->finalResult, true);
  EXPECT_EQ(storageInfo->filePath, "test_filepath");
  EXPECT_EQ(storageInfo->availableStorageTypes, 1);
}

std::unique_ptr<StorageInfo> generateStorageInfo() {
  auto storageInfo = std::make_unique<StorageInfo>();
  storageInfo->type = static_cast<StorageInfo::Type>(1);
  storageInfo->mountPoint = "test_mountpoint";
  storageInfo->finalResult = true;
  storageInfo->filePath = "test_filepath";
  storageInfo->availableStorageTypes = 1;
  return std::move(storageInfo);
}

void verifyStorageInfoPb(const PbStorageInfo* pbStorageInfo) {
  EXPECT_EQ(pbStorageInfo->type(), 1);
  EXPECT_EQ(pbStorageInfo->mountpoint(), "test_mountpoint");
  EXPECT_EQ(pbStorageInfo->finalresult(), true);
  EXPECT_EQ(pbStorageInfo->filepath(), "test_filepath");
  EXPECT_EQ(pbStorageInfo->availablestoragetypes(), 1);
}

std::unique_ptr<PbPartitionLocation> generateBasicPartitionLocationPb() {
  auto pbPartitionLocation = std::make_unique<PbPartitionLocation>();
  pbPartitionLocation->set_id(1);
  pbPartitionLocation->set_epoch(101);
  pbPartitionLocation->set_host("test_host");
  pbPartitionLocation->set_rpcport(1001);
  pbPartitionLocation->set_pushport(1002);
  pbPartitionLocation->set_fetchport(1003);
  pbPartitionLocation->set_replicateport(1004);
  return std::move(pbPartitionLocation);
}

void verifyBasicPartitionLocation(const PartitionLocation* partitionLocation) {
  EXPECT_EQ(partitionLocation->id, 1);
  EXPECT_EQ(partitionLocation->epoch, 101);
  EXPECT_EQ(partitionLocation->host, "test_host");
  EXPECT_EQ(partitionLocation->rpcPort, 1001);
  EXPECT_EQ(partitionLocation->pushPort, 1002);
  EXPECT_EQ(partitionLocation->fetchPort, 1003);
  EXPECT_EQ(partitionLocation->replicatePort, 1004);
}

std::unique_ptr<PartitionLocation> generateBasicPartitionLocation() {
  auto partitionLocation = std::make_unique<PartitionLocation>();
  partitionLocation->id = 1;
  partitionLocation->epoch = 101;
  partitionLocation->host = "test_host";
  partitionLocation->rpcPort = 1001;
  partitionLocation->pushPort = 1002;
  partitionLocation->fetchPort = 1003;
  partitionLocation->replicatePort = 1004;
  return std::move(partitionLocation);
}

void verifyBasicPartitionLocationPb(
    const PbPartitionLocation* pbPartitionLocation) {
  EXPECT_EQ(pbPartitionLocation->id(), 1);
  EXPECT_EQ(pbPartitionLocation->epoch(), 101);
  EXPECT_EQ(pbPartitionLocation->host(), "test_host");
  EXPECT_EQ(pbPartitionLocation->rpcport(), 1001);
  EXPECT_EQ(pbPartitionLocation->pushport(), 1002);
  EXPECT_EQ(pbPartitionLocation->fetchport(), 1003);
  EXPECT_EQ(pbPartitionLocation->replicateport(), 1004);
}

TEST(PartitionLocationTest, storageInfoFromPb) {
  auto pbStorageInfo = generateStorageInfoPb();
  auto storageInfo = StorageInfo::fromPb(*pbStorageInfo);
  verifyStorageInfo(storageInfo.get());
}

TEST(PartitionLocationTest, storageInfoToProto) {
  auto storageInfo = generateStorageInfo();
  auto pbStorageInfo = storageInfo->toPb();
  verifyStorageInfoPb(pbStorageInfo.get());
}

TEST(PartitionLocationTest, fromPbWithoutPeer) {
  auto pbPartitionLocation = generateBasicPartitionLocationPb();
  pbPartitionLocation->set_mode(PbPartitionLocation_Mode_Primary);
  auto pbStorageInfo = generateStorageInfoPb();
  pbPartitionLocation->set_allocated_storageinfo(pbStorageInfo.release());

  auto partitionLocation = PartitionLocation::fromPb(*pbPartitionLocation);

  verifyBasicPartitionLocation(partitionLocation.get());
  EXPECT_EQ(partitionLocation->mode, PartitionLocation::Mode::PRIMARY);
  verifyStorageInfo(partitionLocation->storageInfo.get());
}

TEST(PartitionLocationTest, fromPbWithPeer) {
  auto pbPartitionLocationPrimary = generateBasicPartitionLocationPb();
  pbPartitionLocationPrimary->set_mode(PbPartitionLocation_Mode_Primary);
  auto pbStorageInfoPrimary = generateStorageInfoPb();
  pbPartitionLocationPrimary->set_allocated_storageinfo(
      pbStorageInfoPrimary.release());

  auto pbPartitionLocationReplica = generateBasicPartitionLocationPb();
  pbPartitionLocationReplica->set_mode(PbPartitionLocation_Mode_Replica);
  auto pbStorageInfoReplica = generateStorageInfoPb();
  pbPartitionLocationReplica->set_allocated_storageinfo(
      pbStorageInfoReplica.release());

  pbPartitionLocationPrimary->set_allocated_peer(
      pbPartitionLocationReplica.release());

  auto partitionLocationPrimary =
      PartitionLocation::fromPb(*pbPartitionLocationPrimary);

  verifyBasicPartitionLocation(partitionLocationPrimary.get());
  EXPECT_EQ(partitionLocationPrimary->mode, PartitionLocation::Mode::PRIMARY);
  verifyStorageInfo(partitionLocationPrimary->storageInfo.get());

  auto partitionLocationReplica = partitionLocationPrimary->replicaPeer.get();
  verifyBasicPartitionLocation(partitionLocationReplica);
  EXPECT_EQ(partitionLocationReplica->mode, PartitionLocation::Mode::REPLICA);
  verifyStorageInfo(partitionLocationReplica->storageInfo.get());
}

TEST(PartitionLocationTest, toProtoWithoutPeer) {
  auto partitionLocation = generateBasicPartitionLocation();
  partitionLocation->mode = PartitionLocation::PRIMARY;
  partitionLocation->storageInfo = generateStorageInfo();

  auto pbPartitionLocation = partitionLocation->toPb();

  verifyBasicPartitionLocationPb(pbPartitionLocation.get());
  EXPECT_EQ(pbPartitionLocation->mode(), PbPartitionLocation_Mode_Primary);
  verifyStorageInfoPb(&pbPartitionLocation->storageinfo());
}

TEST(PartitionLocationTest, toProtoWithPeer) {
  auto partitionLocationPrimary = generateBasicPartitionLocation();
  partitionLocationPrimary->mode = PartitionLocation::PRIMARY;
  partitionLocationPrimary->storageInfo = generateStorageInfo();

  auto partitionLocationReplica = generateBasicPartitionLocation();
  partitionLocationReplica->mode = PartitionLocation::REPLICA;
  partitionLocationReplica->storageInfo = generateStorageInfo();

  partitionLocationPrimary->replicaPeer = std::move(partitionLocationReplica);

  auto pbPartitionLocationPrimary = partitionLocationPrimary->toPb();

  verifyBasicPartitionLocationPb(pbPartitionLocationPrimary.get());
  EXPECT_EQ(
      pbPartitionLocationPrimary->mode(), PbPartitionLocation_Mode_Primary);
  verifyStorageInfoPb(&pbPartitionLocationPrimary->storageinfo());

  auto pbPartitionLocationReplica = &pbPartitionLocationPrimary->peer();
  verifyBasicPartitionLocationPb(pbPartitionLocationReplica);
  EXPECT_EQ(
      pbPartitionLocationReplica->mode(), PbPartitionLocation_Mode_Replica);
  verifyStorageInfoPb(&pbPartitionLocationReplica->storageinfo());
}

TEST(PartitionLocationTest, hasPeer) {
  auto partitionLocationWithoutPeer = generateBasicPartitionLocation();
  partitionLocationWithoutPeer->mode = PartitionLocation::PRIMARY;
  partitionLocationWithoutPeer->storageInfo = generateStorageInfo();

  EXPECT_FALSE(partitionLocationWithoutPeer->hasPeer());

  auto partitionLocationWithPeer = generateBasicPartitionLocation();
  partitionLocationWithPeer->mode = PartitionLocation::PRIMARY;
  partitionLocationWithPeer->storageInfo = generateStorageInfo();

  auto partitionLocationReplica = generateBasicPartitionLocation();
  partitionLocationReplica->mode = PartitionLocation::REPLICA;
  partitionLocationReplica->storageInfo = generateStorageInfo();

  partitionLocationWithPeer->replicaPeer = std::move(partitionLocationReplica);

  EXPECT_TRUE(partitionLocationWithPeer->hasPeer());
}

TEST(PartitionLocationTest, getPeer) {
  auto partitionLocationPrimary = generateBasicPartitionLocation();
  partitionLocationPrimary->mode = PartitionLocation::PRIMARY;
  partitionLocationPrimary->storageInfo = generateStorageInfo();

  auto partitionLocationReplica = generateBasicPartitionLocation();
  partitionLocationReplica->mode = PartitionLocation::REPLICA;
  partitionLocationReplica->storageInfo = generateStorageInfo();

  partitionLocationPrimary->replicaPeer = std::move(partitionLocationReplica);

  const auto* peer = partitionLocationPrimary->getPeer();
  EXPECT_NE(peer, nullptr);
  EXPECT_EQ(peer->mode, PartitionLocation::Mode::REPLICA);
  verifyBasicPartitionLocation(peer);

  auto partitionLocationWithoutPeer = generateBasicPartitionLocation();
  partitionLocationWithoutPeer->mode = PartitionLocation::PRIMARY;
  partitionLocationWithoutPeer->storageInfo = generateStorageInfo();

  EXPECT_EQ(partitionLocationWithoutPeer->getPeer(), nullptr);
}

TEST(PartitionLocationTest, hostAndFetchPort) {
  auto partitionLocation = generateBasicPartitionLocation();
  partitionLocation->host = "test_host";
  partitionLocation->fetchPort = 1003;

  std::string expected = "test_host:1003";
  EXPECT_EQ(partitionLocation->hostAndFetchPort(), expected);
}

TEST(PartitionLocationTest, fromPbWithMapIdBitmap) {
  auto pbPartitionLocation = generateBasicPartitionLocationPb();
  pbPartitionLocation->set_mode(PbPartitionLocation_Mode_Primary);
  auto pbStorageInfo = generateStorageInfoPb();
  pbPartitionLocation->set_allocated_storageinfo(pbStorageInfo.release());

  // Create a roaring bitmap, serialize it in portable format, set on the proto.
  roaring::Roaring bitmap;
  bitmap.add(1);
  bitmap.add(3);
  bitmap.add(5);
  bitmap.add(100);
  size_t size = bitmap.portableSizeInBytes();
  std::string bytes(size, '\0');
  bitmap.portableSerialize(&bytes[0]);
  pbPartitionLocation->set_mapidbitmap(bytes);

  auto partitionLocation = PartitionLocation::fromPb(*pbPartitionLocation);

  verifyBasicPartitionLocation(partitionLocation.get());
  ASSERT_NE(partitionLocation->mapIdBitMap, nullptr);
  EXPECT_TRUE(partitionLocation->mapIdBitMap->contains(1));
  EXPECT_TRUE(partitionLocation->mapIdBitMap->contains(3));
  EXPECT_TRUE(partitionLocation->mapIdBitMap->contains(5));
  EXPECT_TRUE(partitionLocation->mapIdBitMap->contains(100));
  EXPECT_FALSE(partitionLocation->mapIdBitMap->contains(2));
  EXPECT_FALSE(partitionLocation->mapIdBitMap->contains(4));
  EXPECT_FALSE(partitionLocation->mapIdBitMap->contains(99));
}

TEST(PartitionLocationTest, fromPbWithEmptyMapIdBitmap) {
  auto pbPartitionLocation = generateBasicPartitionLocationPb();
  pbPartitionLocation->set_mode(PbPartitionLocation_Mode_Primary);
  auto pbStorageInfo = generateStorageInfoPb();
  pbPartitionLocation->set_allocated_storageinfo(pbStorageInfo.release());
  // Don't set mapIdBitmap - it should be empty by default.

  auto partitionLocation = PartitionLocation::fromPb(*pbPartitionLocation);

  EXPECT_EQ(partitionLocation->mapIdBitMap, nullptr);
}

TEST(PartitionLocationTest, toPbWithMapIdBitmap) {
  auto partitionLocation = generateBasicPartitionLocation();
  partitionLocation->mode = PartitionLocation::PRIMARY;
  partitionLocation->storageInfo = generateStorageInfo();

  auto bitmap = std::make_shared<roaring::Roaring>();
  bitmap->add(2);
  bitmap->add(4);
  bitmap->add(6);
  partitionLocation->mapIdBitMap = bitmap;

  auto pbPartitionLocation = partitionLocation->toPb();

  // Verify bitmap was serialized.
  const auto& bitmapBytes = pbPartitionLocation->mapidbitmap();
  ASSERT_FALSE(bitmapBytes.empty());

  // Deserialize and verify contents.
  roaring::Roaring deserialized =
      roaring::Roaring::readSafe(bitmapBytes.data(), bitmapBytes.size());
  EXPECT_TRUE(deserialized.contains(2));
  EXPECT_TRUE(deserialized.contains(4));
  EXPECT_TRUE(deserialized.contains(6));
  EXPECT_FALSE(deserialized.contains(1));
}

TEST(PartitionLocationTest, toPbWithNullBitmap) {
  auto partitionLocation = generateBasicPartitionLocation();
  partitionLocation->mode = PartitionLocation::PRIMARY;
  partitionLocation->storageInfo = generateStorageInfo();
  partitionLocation->mapIdBitMap = nullptr;

  auto pbPartitionLocation = partitionLocation->toPb();

  EXPECT_TRUE(pbPartitionLocation->mapidbitmap().empty());
}

TEST(PartitionLocationTest, bitmapRoundTrip) {
  auto partitionLocation = generateBasicPartitionLocation();
  partitionLocation->mode = PartitionLocation::PRIMARY;
  partitionLocation->storageInfo = generateStorageInfo();

  auto bitmap = std::make_shared<roaring::Roaring>();
  for (int i = 0; i < 1000; i += 3) {
    bitmap->add(i);
  }
  partitionLocation->mapIdBitMap = bitmap;

  // Serialize to protobuf.
  auto pb = partitionLocation->toPb();

  // Deserialize from protobuf.
  auto restored = PartitionLocation::fromPb(*pb);

  ASSERT_NE(restored->mapIdBitMap, nullptr);
  EXPECT_EQ(*restored->mapIdBitMap, *partitionLocation->mapIdBitMap);
}

TEST(PartitionLocationTest, copyConstructorWithBitmap) {
  auto partitionLocation = generateBasicPartitionLocation();
  partitionLocation->mode = PartitionLocation::PRIMARY;
  partitionLocation->storageInfo = generateStorageInfo();

  auto bitmap = std::make_shared<roaring::Roaring>();
  bitmap->add(10);
  bitmap->add(20);
  partitionLocation->mapIdBitMap = bitmap;

  // Copy.
  PartitionLocation copy(*partitionLocation);

  // Verify copy has the same bitmap (shared_ptr, so same object).
  ASSERT_NE(copy.mapIdBitMap, nullptr);
  EXPECT_TRUE(copy.mapIdBitMap->contains(10));
  EXPECT_TRUE(copy.mapIdBitMap->contains(20));
}
