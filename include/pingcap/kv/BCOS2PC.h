#pragma once

#include <pingcap/Exception.h>
#include <pingcap/kv/2pc.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/LockResolver.h>

#include <cmath>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace pingcap {
namespace kv {

struct Txn;

struct BCOSTwoPhaseCommitter;

using BCOSTwoPhaseCommitterPtr = std::shared_ptr<BCOSTwoPhaseCommitter>;

struct PreWriteResult {
  uint64_t start_ts = 0;
  std::string primary_lock;
};
struct BCOSTwoPhaseCommitter
    : public std::enable_shared_from_this<BCOSTwoPhaseCommitter> {
private:
  std::unordered_map<std::string, std::string> mutations;
  // FIXME: std::vector<std::string_view> keys;
  std::vector<std::string> keys;
  uint64_t start_ts = 0;

  std::shared_mutex commit_ts_mu;
  uint64_t commit_ts = 0;
  uint64_t min_commit_ts = 0;
  uint64_t max_commit_ts = 0;

  // Used to calculate max_commit_ts
  std::chrono::milliseconds start_time;

  Cluster *cluster;

  std::unordered_map<uint64_t, int> region_txn_size;
  uint64_t txn_size = 0;

  int lock_ttl = 0;

  std::string primary_lock;
  // commited means primary key has been written to kv stores.
  bool commited;

  // Only for test now
  bool use_async_commit = false;

  TTLManager<BCOSTwoPhaseCommitter> ttl_manager;

  Logger *log;

  friend class TTLManager<BCOSTwoPhaseCommitter>;
  struct BatchKeys {
    RegionVerID region;
    std::vector<std::string_view> keys;
    bool is_primary;
    BatchKeys(const RegionVerID &region_, std::vector<std::string_view> keys_,
              bool is_primary_ = false)
        : region(region_), keys(std::move(keys_)), is_primary(is_primary_) {}
  };
  using GroupsType = std::unordered_map<RegionVerID, std::vector<std::string>>;
  using BatchesType = std::vector<BatchKeys>;
  std::shared_ptr<GroupsType> m_groups = nullptr;

public:
  BCOSTwoPhaseCommitter(
      Cluster *_cluster, const std::string_view &_primary_lock,
      std::unordered_map<std::string, std::string> &&_mutations);

  void prewriteKeys(uint64_t _start_ts) {
    start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch());
    start_ts = _start_ts;
    Backoffer prewrite_bo(prewriteMaxBackoff);
    try {
      prewriteKeys(prewrite_bo, keys);
    } catch (Exception &e) {
      if (!commited) {
        // TODO: Rollback keys.
        log->warning("prewrite failed, message:" + std::string(e.what()) + ":" +
                     e.message());
        rollbackKeys(prewrite_bo, keys);
      }
      log->warning("write commit exception: " + e.displayText());
      throw;
    }
  }
  PreWriteResult prewriteKeys() {
    // the primary_lock should be current_number so we can check block
    // continuity
    prewriteKeys(cluster->pd_client->getTS());
    return PreWriteResult{start_ts, primary_lock};
  }
  void rollback() {
    Backoffer bo(prewriteMaxBackoff);
    doActionOnKeys<ActionRollback>(bo, keys);
    // stop ttl_manager
    ttl_manager.close();
  }
  void commitKeys() {
    Backoffer commit_bo(commitMaxBackoff);
    try {
      commit_ts = cluster->pd_client->getTS();
      doActionOnKeys<ActionCommit>(commit_bo, keys);
      ttl_manager.close();
    } catch (Exception &e) {
      if (!commited) {
        // TODO: Rollback keys.
        rollbackKeys(commit_bo, keys);
        log->warning("commit failed, message:" + std::string(e.what()) + ":" +
                     e.message());
      }
      log->warning("commit exception: " + e.displayText());
      throw;
    }
    log->debug("commitKeys finished, primary_lock: " +
               (mutations.count(primary_lock) ? mutations[primary_lock]
                                              : primary_lock) +
               ", commit_ts: " + std::to_string(commit_ts));
  }

private:
  enum Action {
    ActionPrewrite = 0,
    ActionCommit,
    ActionCleanUp,
    ActionRollback,
  };

  std::shared_ptr<GroupsType>
  prepareGroups(const std::vector<std::string> &cur_keys) {
    Backoffer bo(prewriteMaxBackoff);
    auto [tempGroups, first_region] =
        cluster->region_cache->groupKeysByRegion(bo, cur_keys);
    std::ignore = first_region;
    return std::make_shared<GroupsType>(std::move(tempGroups));
  }
  template <Action action>
  std::shared_ptr<BatchesType>
  prepareBatches(const std::shared_ptr<GroupsType> &cur_groups) {
    auto batches = std::make_shared<BatchesType>();

    uint64_t primary_idx = std::numeric_limits<uint64_t>::max();
    for (auto &group : *cur_groups) {
      uint32_t end = 0;
      for (uint32_t start = 0; start < group.second.size(); start = end) {
        uint64_t size = 0;
        std::vector<std::string_view> sub_keys;
        for (end = start;
             end < group.second.size() && size < txnCommitBatchSize; end++) {
          auto &key = group.second[end];
          size += key.size();
          if constexpr (action == ActionPrewrite)
            size += mutations[key].size();

          if (key == primary_lock)
            primary_idx = batches->size();
          sub_keys.push_back(key);
        }
        batches->emplace_back(BatchKeys(group.first, sub_keys));
      }
    }
    if (primary_idx != std::numeric_limits<uint64_t>::max() &&
        primary_idx != 0) {
      std::swap(batches->at(0), batches->at(primary_idx));
      batches->at(0).is_primary = true;
    }
    return batches;
  }

  void prewriteKeys(Backoffer &bo, const std::vector<std::string> &keys) {
    doActionOnKeys<ActionPrewrite>(bo, keys);
  }
  void rollbackKeys(Backoffer &bo, const std::vector<std::string> &keys) {
    doActionOnKeys<ActionRollback>(bo, keys);
  }

  void commitKeys(Backoffer &bo, const std::vector<std::string> &keys) {
    doActionOnKeys<ActionCommit>(bo, keys);
  }

  template <Action action>
  void doActionOnKeys(Backoffer &bo, const std::vector<std::string> &cur_keys) {
    auto groups = m_groups;
    if (cur_keys.size() != keys.size()) {
      groups = prepareGroups(cur_keys);
    }
    auto batches = prepareBatches<action>(groups);

    if constexpr (action == ActionCommit || action == ActionCleanUp) {
      if constexpr (action == ActionCommit) {
        fiu_do_on("all commit fail", return );
      }
      doActionOnBatches<action>(
          bo, std::vector<BatchKeys>(batches->begin(), batches->begin() + 1));
      batches =
          std::make_shared<BatchesType>(batches->begin() + 1, batches->end());
    }
    if (action != ActionCommit || !fiu_fail("rest commit fail")) {
      doActionOnBatches<action>(bo, *batches);
    }
  }

  template <Action action>
  void doActionOnBatches(Backoffer &bo, const std::vector<BatchKeys> &batches) {
    if constexpr (action == ActionPrewrite) {
      asyncPrewriteBatches(bo, batches);
    }
    for (const auto &batch : batches) {
      if constexpr (action == ActionRollback) {
        rollbackSingleBatch(bo, batch);
      }
      // else if constexpr (action == ActionPrewrite)
      // {
      //     region_txn_size[batch.region.id] = batch.keys.size();
      //     prewriteSingleBatch(bo, batch);
      // }
      else if constexpr (action == ActionCommit) {
        commitSingleBatch(bo, batch);
      }
    }
  }

  void prewriteSingleBatch(Backoffer &bo, const BatchKeys &batch);

  void asyncPrewriteBatches(Backoffer &bo,
                            const std::vector<BatchKeys> &batches);
  void rollbackSingleBatch(Backoffer &bo, const BatchKeys &batch);

  void commitSingleBatch(Backoffer &bo, const BatchKeys &batch);
};

} // namespace kv
} // namespace pingcap
