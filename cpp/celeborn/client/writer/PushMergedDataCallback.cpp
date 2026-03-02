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

#include "celeborn/client/writer/PushMergedDataCallback.h"
#include "celeborn/conf/CelebornConf.h"

namespace celeborn {
namespace client {

std::shared_ptr<PushMergedDataCallback> PushMergedDataCallback::create(
    int shuffleId,
    int mapId,
    int attemptId,
    int numMappers,
    int numPartitions,
    const std::string& mapKey,
    int groupedBatchId,
    std::vector<DataBatch> batches,
    std::shared_ptr<PushState> pushState,
    std::weak_ptr<ShuffleClientImpl> weakClient,
    int remainingReviveTimes,
    const PushState::AddressPair& addressPair) {
  return std::shared_ptr<PushMergedDataCallback>(new PushMergedDataCallback(
      shuffleId,
      mapId,
      attemptId,
      numMappers,
      numPartitions,
      mapKey,
      groupedBatchId,
      std::move(batches),
      pushState,
      weakClient,
      remainingReviveTimes,
      addressPair));
}

PushMergedDataCallback::PushMergedDataCallback(
    int shuffleId,
    int mapId,
    int attemptId,
    int numMappers,
    int numPartitions,
    const std::string& mapKey,
    int groupedBatchId,
    std::vector<DataBatch> batches,
    std::shared_ptr<PushState> pushState,
    std::weak_ptr<ShuffleClientImpl> weakClient,
    int remainingReviveTimes,
    const PushState::AddressPair& addressPair)
    : shuffleId_(shuffleId),
      mapId_(mapId),
      attemptId_(attemptId),
      numMappers_(numMappers),
      numPartitions_(numPartitions),
      mapKey_(mapKey),
      groupedBatchId_(groupedBatchId),
      batches_(std::move(batches)),
      pushState_(pushState),
      weakClient_(weakClient),
      remainingReviveTimes_(remainingReviveTimes),
      addressPair_(addressPair) {}

void PushMergedDataCallback::onSuccess(
    std::unique_ptr<memory::ReadOnlyByteBuffer> response) {
  auto sharedClient = weakClient_.lock();
  if (!sharedClient) {
    LOG(WARNING) << "ShuffleClientImpl has expired when "
                    "PushMergedDataCallbackOnSuccess, ignored, shuffle "
                 << shuffleId_ << " map " << mapId_ << " attempt " << attemptId_
                 << " groupedBatchId " << groupedBatchId_ << ".";
    return;
  }

  if (response->remainingSize() <= 0) {
    pushState_->onSuccess(addressPair_.first);
    pushState_->removeBatch(groupedBatchId_, addressPair_.first);
    return;
  }

  protocol::StatusCode reason =
      static_cast<protocol::StatusCode>(response->read<uint8_t>());
  switch (reason) {
    case protocol::StatusCode::MAP_ENDED: {
      auto mapperEndSet = sharedClient->mapperEndSets().computeIfAbsent(
          shuffleId_,
          []() { return std::make_shared<utils::ConcurrentHashSet<int>>(); });
      mapperEndSet->insert(mapId_);
      pushState_->removeBatch(groupedBatchId_, addressPair_.first);
      break;
    }
    case protocol::StatusCode::SOFT_SPLIT: {
      VLOG(1) << "Push merged data to " << addressPair_.first
              << " soft split required for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " groupedBatchId "
              << groupedBatchId_ << ".";
      // Trigger revive for all batches in this merged push.
      for (const auto& batch : batches_) {
        if (!ShuffleClientImpl::newerPartitionLocationExists(
                sharedClient->getPartitionLocationMap(shuffleId_).value(),
                batch.loc->id,
                batch.loc->epoch)) {
          auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
              shuffleId_,
              mapId_,
              attemptId_,
              batch.loc->id,
              batch.loc->epoch,
              batch.loc,
              protocol::StatusCode::SOFT_SPLIT);
          sharedClient->addRequestToReviveManager(reviveRequest);
        }
      }
      pushState_->onSuccess(addressPair_.first);
      pushState_->removeBatch(groupedBatchId_, addressPair_.first);
      break;
    }
    case protocol::StatusCode::HARD_SPLIT: {
      VLOG(1) << "Push merged data to " << addressPair_.first
              << " hard split required for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " groupedBatchId "
              << groupedBatchId_ << ".";
      reviveAndRetryPushMergedData(
          *sharedClient, protocol::StatusCode::HARD_SPLIT);
      break;
    }
    case protocol::StatusCode::PUSH_DATA_SUCCESS_PRIMARY_CONGESTED:
    case protocol::StatusCode::PUSH_DATA_SUCCESS_REPLICA_CONGESTED: {
      VLOG(1) << "Push merged data to " << addressPair_.first
              << " congestion required for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " groupedBatchId "
              << groupedBatchId_ << ".";
      pushState_->onCongestControl(addressPair_.first);
      pushState_->removeBatch(groupedBatchId_, addressPair_.first);
      break;
    }
    default: {
      LOG(WARNING)
          << "unhandled PushMergedData success protocol::StatusCode: "
          << reason;
    }
  }
}

void PushMergedDataCallback::onFailure(
    std::unique_ptr<std::exception> exception) {
  auto sharedClient = weakClient_.lock();
  if (!sharedClient) {
    LOG(WARNING) << "ShuffleClientImpl has expired when "
                    "PushMergedDataCallbackOnFailure, ignored, shuffle "
                 << shuffleId_ << " map " << mapId_ << " attempt " << attemptId_
                 << " groupedBatchId " << groupedBatchId_ << ".";
    return;
  }
  if (pushState_->exceptionExists()) {
    return;
  }

  LOG(ERROR) << "Push merged data to " << addressPair_.first
             << " failed for shuffle " << shuffleId_ << " map " << mapId_
             << " attempt " << attemptId_ << " groupedBatchId "
             << groupedBatchId_ << ", remain revive times "
             << remainingReviveTimes_;

  if (remainingReviveTimes_ <= 0) {
    pushState_->setException(std::move(exception));
    return;
  }

  if (sharedClient->mapperEnded(shuffleId_, mapId_)) {
    pushState_->removeBatch(groupedBatchId_, addressPair_.first);
    LOG(INFO) << "Push merged data to " << addressPair_.first
              << " failed but mapper already ended for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_
              << " groupedBatchId " << groupedBatchId_
              << ", remain revive times " << remainingReviveTimes_ << ".";
    return;
  }
  remainingReviveTimes_--;
  protocol::StatusCode cause =
      protocol::StatusCode::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY;
  reviveAndRetryPushMergedData(*sharedClient, cause);
}

void PushMergedDataCallback::reviveAndRetryPushMergedData(
    ShuffleClientImpl& shuffleClient,
    protocol::StatusCode cause) {
  // Create revive requests for all batches.
  std::vector<std::shared_ptr<protocol::ReviveRequest>> reviveRequests;
  for (const auto& batch : batches_) {
    auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
        shuffleId_,
        mapId_,
        attemptId_,
        batch.loc->id,
        batch.loc->epoch,
        batch.loc,
        cause);
    reviveRequests.push_back(reviveRequest);
    shuffleClient.addRequestToReviveManager(reviveRequest);
  }

  long dueTimeMs = utils::currentTimeMillis() +
      shuffleClient.conf_->clientRpcRequestPartitionLocationRpcAskTimeout() /
          utils::MS(1);

  // Clone batches for the retry task.
  std::vector<DataBatch> batchesCopy;
  for (const auto& batch : batches_) {
    DataBatch copy;
    copy.loc = batch.loc;
    copy.batchId = batch.batchId;
    copy.body = batch.body->clone();
    batchesCopy.push_back(std::move(copy));
  }

  shuffleClient.addPushDataRetryTask(
      [weakClient = this->weakClient_,
       shuffleId = this->shuffleId_,
       mapId = this->mapId_,
       attemptId = this->attemptId_,
       numMappers = this->numMappers_,
       numPartitions = this->numPartitions_,
       groupedBatchId = this->groupedBatchId_,
       batches = std::move(batchesCopy),
       pushState = this->pushState_,
       reviveRequests = std::move(reviveRequests),
       remainingReviveTimes = this->remainingReviveTimes_,
       dueTimeMs]() mutable {
        auto sharedClient = weakClient.lock();
        if (!sharedClient) {
          LOG(WARNING) << "ShuffleClientImpl has expired when "
                          "PushMergedDataFailureCallback, ignored, shuffleId "
                       << shuffleId;
          return;
        }
        sharedClient->submitRetryPushMergedData(
            shuffleId,
            mapId,
            attemptId,
            numMappers,
            numPartitions,
            groupedBatchId,
            std::move(batches),
            pushState,
            reviveRequests,
            remainingReviveTimes,
            dueTimeMs);
      });
}

} // namespace client
} // namespace celeborn
