#include <boost/context/fixedsize_stack.hpp>
#include <pingcap/RedactHelpers.h>
#include <pingcap/kv/BCOS2PC.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/kv/Txn.h>
#include <pingcap/pd/Oracle.h>
#include <string>

using namespace std;

namespace pingcap {
namespace kv {
vector<string> convert(const vector<string_view> &o) {
  vector<string> ret;
  for (auto &v : o) {
    ret.emplace_back(v.data(), v.size());
  }
  return std::move(ret);
}
BCOSTwoPhaseCommitter::BCOSTwoPhaseCommitter(
    Cluster *_cluster, const std::string_view &_primary_lock,
    std::unordered_map<std::string, std::string> &&_mutations,
    size_t _coroutineStack, int32_t _maxRetry)
    : mutations(std::move(_mutations)), cluster(_cluster), maxRetry(_maxRetry),
      coroutineStack(_coroutineStack), log(&Logger::get("pingcap.bcos2pc")),
      logStream(*log) {
  start_ts = cluster->pd_client->getTS();
  start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
  keys.reserve(mutations.size());
  for (auto &item : mutations) {
    keys.emplace_back(item.first);
  }
  commited = false;
  primary_lock = std::string(_primary_lock);
  txn_size = mutations.size();
  // TODO: use right lock_ttl
  // currently prewrite is not concurrent, so the right lock_ttl is not enough
  // for prewrite to complete lock_ttl = txnLockTTL(txn->start_time, txn_size);
  lock_ttl = defaultLockTTL;
  if (txn_size > ttlManagerRunThreshold) {
    lock_ttl = managedLockTTL;
  }
  m_groups = prepareGroups(keys);
}

void BCOSTwoPhaseCommitter::prewriteSingleBatch(Backoffer &bo,
                                                const BatchKeys &batch) {
  uint64_t batch_txn_size = region_txn_size[batch.region.id];

  for (;;) {
    auto req = std::make_shared<kvrpcpb::PrewriteRequest>();
    for (const auto &key : batch.keys) {

      auto *mut = req->add_mutations();
      std::string skey(key);
      if (mutations[skey].empty()) {
        mut->set_op(kvrpcpb::Op::Del);
      } else {
        mut->set_value(mutations[skey]);
      }
      mut->set_key(std::move(skey));
    }
    req->set_primary_lock(primary_lock);
    req->set_start_version(start_ts);
    req->set_lock_ttl(lock_ttl);
    req->set_txn_size(batch_txn_size);
    req->set_max_commit_ts(max_commit_ts);

    // TODO: set right min_commit_ts for pessimistic lock
    req->set_min_commit_ts(start_ts + 1);

    fiu_do_on("invalid_max_commit_ts",
              { req->set_max_commit_ts(min_commit_ts - 1); });

    std::shared_ptr<kvrpcpb::PrewriteResponse> response;
    RegionClient region_client(cluster, batch.region);
    try {
      response = region_client.sendReqToRegion(bo, req);
    } catch (Exception &e) {
      log->warning("prewriteSingleBatch exception, " + std::string(e.what()) +
                   ":" + e.message());
      // Region Error.
      bo.backoff(boRegionMiss, e);
      prewriteKeys(bo, convert(batch.keys));
      return;
    }

    if (response->errors_size() != 0) {
      std::vector<LockPtr> locks;
      int size = response->errors_size();
      for (int i = 0; i < size; i++) {
        const auto &err = response->errors(i);
        if (err.has_already_exist()) {
          log->warning("prewriteSingleBatch error, errors: key : " +
                       Redact::keyToDebugString(err.already_exist().key()) +
                       " has existed.");
          throw Exception(
              "key : " + Redact::keyToDebugString(err.already_exist().key()) +
                  " has existed.",
              LogicalError);
        }
        auto lock = extractLockFromKeyErr(err);
        locks.push_back(lock);
      }
      auto ms_before_expired =
          cluster->lock_resolver->resolveLocksForWrite(bo, start_ts, locks);
      if (ms_before_expired > 0) {
        bo.backoffWithMaxSleep(
            boTxnLock, ms_before_expired,
            Exception("2PC prewrite locked: " + std::to_string(locks.size()),
                      LockError));
      }
      continue;
    } else {
      if (batch.keys[0] == primary_lock) {
        // After writing the primary key, if the size of the transaction is
        // large than 32M, start the ttlManager. The ttlManager will be closed
        // in tikvTxn.Commit().
        if (txn_size > ttlManagerRunThreshold) {
          ttl_manager.run(shared_from_this());
        }
      }
    }

    return;
  }
}

void BCOSTwoPhaseCommitter::asyncPrewriteBatches(
    Backoffer &bo, const std::vector<BatchKeys> &batches) {
  // create request
  auto start = std::chrono::system_clock::now();
  using coro_t = boost::coroutines2::coroutine<size_t>;
  std::vector<shared_ptr<kvrpcpb::PrewriteRequest>> requests(batches.size(),
                                                             nullptr);
  std::vector<shared_ptr<kvrpcpb::PrewriteResponse>> responses(batches.size(),
                                                               nullptr);
  std::vector<coro_t::push_type> coroutines;
  grpc::CompletionQueue cq;
  for (size_t i = 0; i < batches.size(); ++i) {
    auto &batch = batches[i];
    uint64_t batch_txn_size = batch.keys.size();
    requests[i] = std::make_shared<kvrpcpb::PrewriteRequest>();
    auto &req = requests[i];

    for (const auto &key : batch.keys) {
      auto *mut = req->add_mutations();
      auto skey = std::string(key);
      if (mutations[skey].empty()) {
        mut->set_op(kvrpcpb::Op::Del);
      } else {
        mut->set_value(mutations[skey]);
      }
      mut->set_key(std::move(skey));
    }
    req->set_primary_lock(primary_lock);
    req->set_start_version(start_ts);
    req->set_lock_ttl(lock_ttl);
    req->set_txn_size(batch_txn_size);
    req->set_max_commit_ts(max_commit_ts);

    // set right min_commit_ts for pessimistic lock
    req->set_min_commit_ts(start_ts + 1);
    fiu_do_on("invalid_max_commit_ts",
              { req->set_max_commit_ts(min_commit_ts - 1); });

    coroutines.emplace_back(
        boost::coroutines2::fixedsize_stack(coroutineStack),
        [&, index = i](coro_t::pull_type &in) {
          log->trace("asyncPrewriteBatches start,index=" + to_string(index));
          try {
            RegionClient region_client(cluster, batches[index].region);
            responses[index] = region_client.asyncSendReqToRegion(
                bo, requests[index], &cq, in);
          } catch (Exception &e) {
            log->warning("asyncPrewriteBatches exception, " +
                         std::string(e.what()) + ":" + e.message());
            // Region Error.
            bo.backoff(boRegionMiss, e);
            cluster->region_cache->getRegionByID(bo, batches[index].region);
            prewriteKeys(bo, convert(batches[index].keys));
          }
          log->trace("asyncPrewriteBatches finished,index=" + to_string(index));

          for (;;) {
            in();
            auto start = std::chrono::system_clock::now();
            log->trace("prewrite retry,index=" + to_string(index));
            prewriteKeys(bo, convert(batches[index].keys));
            auto retryEnd = std::chrono::system_clock::now();
            logStream.trace()
                << "prewrite retry finished,index=" << to_string(index)
                << ", retryTime(ms)="
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       retryEnd - start)
                       .count()
                << std::endl;
          }
        });
    coroutines[i](i);
  }
  auto sendEnd = std::chrono::system_clock::now();
  log->debug(
      "asyncPrewriteBatches requests sent, batches.size=" +
      to_string(batches.size()) + ", primary_lock=" +
      (mutations.count(primary_lock) ? mutations[primary_lock] : primary_lock));
  for (size_t i = 0; i < batches.size(); ++i) { // after finish
    size_t *id = nullptr;
    bool ok = false;
    cq.Next((void **)&id, &ok);
    coroutines[*id](*id);
    // if (!ok) { // retry
    //   log->warning("CompletionQueue got failed retry");
    //   --i;
    // }
  }
  auto receiveEnd = std::chrono::system_clock::now();
  log->trace("asyncPrewriteBatches responses received");
  for (size_t i = 0; i < batches.size(); ++i) {
    auto response = responses[i];
    if (!response) {
      log->warning("asyncPrewriteBatches skip empty response, index=" +
                   to_string(i));
      continue;
    }
    if (response->errors_size() != 0) {
      std::vector<LockPtr> locks;
      int size = response->errors_size();
      for (int j = 0; j < size; j++) {
        const auto &err = response->errors(j);
        if (err.has_already_exist()) {
          log->warning("asyncPrewriteBatches key already exist error, key : " +
                       Redact::keyToDebugString(err.already_exist().key()) +
                       " has existed.");
          throw Exception(
              "key : " + Redact::keyToDebugString(err.already_exist().key()) +
                  " has existed.",
              LogicalError);
        }
        log->trace("asyncPrewriteBatches failed,index=" + to_string(i) +
                   ",error:" + err.ShortDebugString());
        auto lock = extractLockFromKeyErr(err);
        locks.push_back(lock);
      }
      auto ms_before_expired =
          cluster->lock_resolver->resolveLocksForWrite(bo, start_ts, locks);
      if (ms_before_expired > 0) {
        bo.backoffWithMaxSleep(
            boTxnLock, ms_before_expired,
            Exception("2PC prewrite locked: " + std::to_string(locks.size()),
                      LockError));
      }
      log->debug("asyncPrewriteBatches retry batch,index=" + to_string(i));
      // retry
      coroutines[i](i);
      // size_t *id = nullptr;
      // bool ok = false;
      // cq.Next((void **)&id, &ok);
      // assert(*id == i);
      // coroutines[i](i);
      // --i;
      continue;
    } else {
      if (batches[i].keys[0] == primary_lock) {
        // After writing the primary key, if the size of the transaction is
        // large than 32M, start the ttlManager. The ttlManager will be closed
        // in tikvTxn.Commit().
        if (txn_size > ttlManagerRunThreshold) {
          ttl_manager.run(shared_from_this());
        }
      }
    }
  }
  auto processTimeCost = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now() - receiveEnd)
                             .count();
  if (processTimeCost > 10) {
    logStream.debug() << "asyncPrewriteBatches finished, batches.size="
                      << to_string(batches.size()) + ", primary_lock="
                      << (mutations.count(primary_lock)
                              ? mutations[primary_lock]
                              : primary_lock)
                      << ", sendTime(ms)="
                      << std::chrono::duration_cast<std::chrono::milliseconds>(
                             sendEnd - start)
                             .count()
                      << ", receiveTime(ms)="
                      << std::chrono::duration_cast<std::chrono::milliseconds>(
                             receiveEnd - sendEnd)
                             .count()
                      << ", processTime(ms)=" << processTimeCost << std::endl;
  }
}

void BCOSTwoPhaseCommitter::rollbackSingleBatch(Backoffer &bo,
                                                const BatchKeys &batch) {
  auto req = std::make_shared<kvrpcpb::BatchRollbackRequest>();
  for (const auto &key : batch.keys) {
    req->add_keys(std::string(key));
  }
  req->set_start_version(start_ts);

  std::shared_ptr<kvrpcpb::BatchRollbackResponse> response;
  RegionClient region_client(cluster, batch.region);
  try {
    response = region_client.sendReqToRegion(bo, req);
  } catch (Exception &e) { // Region Error.
    log->warning("rollbackSingleBatch exception and retry, " +
                 std::string(e.what()) + ":" + e.message() +
                 ", batch.size=" + to_string(batch.keys.size()));
    bo.backoff(boRegionMiss, e);
    cluster->region_cache->getRegionByID(bo, batch.region);
    rollbackKeys(bo, convert(batch.keys));
    return;
  }
  if (response->has_error()) {
    log->warning("rollbackSingleBatch failed, errors: " +
                 response->error().ShortDebugString());
    throw Exception("meet errors: " + response->error().ShortDebugString(),
                    LockError);
  }
}

void BCOSTwoPhaseCommitter::commitSingleBatch(Backoffer &bo,
                                              const BatchKeys &batch) {
  auto req = std::make_shared<kvrpcpb::CommitRequest>();
  for (const auto &key : batch.keys) {
    req->add_keys(std::string(key));
  }
  req->set_start_version(start_ts);
  req->set_commit_version(commit_ts);

  std::shared_ptr<kvrpcpb::CommitResponse> response;
  RegionClient region_client(cluster, batch.region);
  try {
    response = region_client.sendReqToRegion(bo, req);
  } catch (Exception &e) {
    log->warning("commitSingleBatch failed, " + std::string(e.what()) + ":" +
                 e.message());
    bo.backoff(boRegionMiss, e);
    cluster->region_cache->getRegionByID(bo, batch.region);
    if (isPrimary) {
      commit_ts = cluster->pd_client->getTS();
      commitKeys(bo, convert(batch.keys));
    }
    return;
  }
  if (response->has_error()) {
    log->warning("commitSingleBatch failed, errors: " +
                 response->error().ShortDebugString());
    if (isPrimary) {
      throw Exception("meet errors: " + response->error().ShortDebugString(),
                      LockError);
    }
  }

  commited = true;
}

} // namespace kv
} // namespace pingcap
