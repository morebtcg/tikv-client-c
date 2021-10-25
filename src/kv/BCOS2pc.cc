#include <pingcap/RedactHelpers.h>
#include <pingcap/kv/BCOS2PC.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/kv/Txn.h>
#include <pingcap/pd/Oracle.h>

using namespace std;

namespace pingcap
{
    namespace kv
    {

        BCOSTwoPhaseCommitter::BCOSTwoPhaseCommitter(Cluster *_cluster, const std::string_view &_primary_lock, std::unordered_map<std::string, std::string> &&_mutations)
            : mutations(std::move(_mutations)), cluster(_cluster), log(&Logger::get("pingcap.tikv"))
        {
            start_ts = cluster->pd_client->getTS();
            start_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
            keys.reserve(mutations.size());
            for (auto &item : mutations)
            {
                keys.emplace_back(item.first);
            }
            commited = false;
            primary_lock = std::string(_primary_lock);
            txn_size = mutations.size();
            // TODO: use right lock_ttl
            // currently prewrite is not concurrent, so the right lock_ttl is not enough for prewrite to complete
            // lock_ttl = txnLockTTL(txn->start_time, txn_size);
            lock_ttl = defaultLockTTL;
            if (txn_size > ttlManagerRunThreshold)
            {
                lock_ttl = managedLockTTL;
            }
        }

        void BCOSTwoPhaseCommitter::prewriteSingleBatch(Backoffer &bo, const BatchKeys &batch)
        {
            uint64_t batch_txn_size = region_txn_size[batch.region.id];

            for (;;)
            {
                auto req = std::make_shared<kvrpcpb::PrewriteRequest>();
                for (const std::string &key : batch.keys)
                {
                    auto *mut = req->add_mutations();
                    mut->set_key(key);
                    mut->set_value(mutations[key]);
                }
                req->set_primary_lock(primary_lock);
                req->set_start_version(start_ts);
                req->set_lock_ttl(lock_ttl);
                req->set_txn_size(batch_txn_size);
                req->set_max_commit_ts(max_commit_ts);

                // TODO: set right min_commit_ts for pessimistic lock
                req->set_min_commit_ts(start_ts + 1);

                fiu_do_on("invalid_max_commit_ts", { req->set_max_commit_ts(min_commit_ts - 1); });

                std::shared_ptr<kvrpcpb::PrewriteResponse> response;
                RegionClient region_client(cluster, batch.region);
                try
                {
                    response = region_client.sendReqToRegion(bo, req);
                }
                catch (Exception &e)
                {
                    std::cerr << "prewriteSingleBatch failed, " << e.what() << ":" << e.message() << std::endl;
                    // Region Error.
                    bo.backoff(boRegionMiss, e);
                    prewriteKeys(bo, batch.keys);
                    return;
                }

                if (response->errors_size() != 0)
                {
                    std::vector<LockPtr> locks;
                    int size = response->errors_size();
                    for (int i = 0; i < size; i++)
                    {
                        const auto &err = response->errors(i);
                        if (err.has_already_exist())
                        {
                            std::cerr << "prewriteSingleBatch failed, errors: "
                                      << "key : " + Redact::keyToDebugString(err.already_exist().key()) + " has existed." << std::endl;
                            throw Exception("key : " + Redact::keyToDebugString(err.already_exist().key()) + " has existed.", LogicalError);
                        }
                        auto lock = extractLockFromKeyErr(err);
                        locks.push_back(lock);
                    }
                    auto ms_before_expired = cluster->lock_resolver->resolveLocksForWrite(bo, start_ts, locks);
                    if (ms_before_expired > 0)
                    {
                        bo.backoffWithMaxSleep(
                            boTxnLock, ms_before_expired, Exception("2PC prewrite locked: " + std::to_string(locks.size()), LockError));
                    }
                    continue;
                }
                else
                {
                    if (batch.keys[0] == primary_lock)
                    {
                        // After writing the primary key, if the size of the transaction is large than 32M,
                        // start the ttlManager. The ttlManager will be closed in tikvTxn.Commit().
                        if (txn_size > ttlManagerRunThreshold)
                        {
                            ttl_manager.run(shared_from_this());
                        }
                    }
                }

                return;
            }
        }

        void BCOSTwoPhaseCommitter::asyncPrewriteBatches(Backoffer &bo, const std::vector<BatchKeys> &batches)
        {
            // TODO: create request
            using coro_t = boost::coroutines2::coroutine<size_t>;
            std::vector<shared_ptr<kvrpcpb::PrewriteRequest>> requests(batches.size(), nullptr);
            std::vector<shared_ptr<kvrpcpb::PrewriteResponse>> responses(batches.size(), nullptr);
            std::vector<coro_t::push_type> coroutines;
            grpc::CompletionQueue cq;
            for (size_t i = 0; i < batches.size(); ++i)
            {
                auto &batch = batches[i];
                region_txn_size[batch.region.id] = batch.keys.size();
                uint64_t batch_txn_size = region_txn_size[batch.region.id];
                requests[i] = std::make_shared<kvrpcpb::PrewriteRequest>();
                auto &req = requests[i];

                for (const std::string &key : batch.keys)
                {
                    auto *mut = req->add_mutations();
                    mut->set_key(key);
                    mut->set_value(mutations[key]);
                }
                req->set_primary_lock(primary_lock);
                req->set_start_version(start_ts);
                req->set_lock_ttl(lock_ttl);
                req->set_txn_size(batch_txn_size);
                req->set_max_commit_ts(max_commit_ts);

                // TODO: set right min_commit_ts for pessimistic lock
                req->set_min_commit_ts(start_ts + 1);
                fiu_do_on("invalid_max_commit_ts", { req->set_max_commit_ts(min_commit_ts - 1); });

                coroutines.emplace_back([&, index = i](coro_t::pull_type &in)
                                        {
                                            for (;;)
                                            {
                                                try
                                                {
                                                    RegionClient region_client(cluster, batches[index].region);
                                                    responses[index] = region_client.asyncSendReqToRegion(bo, requests[index], &cq, in);
                                                }
                                                catch (Exception &e)
                                                {
                                                    std::cerr << "prewriteSingleBatch failed, " << e.what() << ":" << e.message() << std::endl;
                                                    // Region Error.
                                                    bo.backoff(boRegionMiss, e);
                                                    prewriteKeys(bo, batches[index].keys);
                                                }
                                                in();
                                            }
                                        });
                coroutines[i](i);
            }
            for (size_t i = 0; i < batches.size(); ++i)
            { // after finish
                size_t *id = nullptr;
                bool ok = false;
                cq.Next((void **)&id, &ok);
                coroutines[*id](*id);
            }
            for (size_t i = 0; i < batches.size(); ++i)
            {
                auto response = responses[i];
                if (response->errors_size() != 0)
                {
                    std::vector<LockPtr> locks;
                    int size = response->errors_size();
                    for (int i = 0; i < size; i++)
                    {
                        const auto &err = response->errors(i);
                        if (err.has_already_exist())
                        {
                            std::cerr << "prewriteSingleBatch failed, errors: "
                                      << "key : " + Redact::keyToDebugString(err.already_exist().key()) + " has existed." << std::endl;
                            throw Exception("key : " + Redact::keyToDebugString(err.already_exist().key()) + " has existed.", LogicalError);
                        }
                        auto lock = extractLockFromKeyErr(err);
                        locks.push_back(lock);
                    }
                    auto ms_before_expired = cluster->lock_resolver->resolveLocksForWrite(bo, start_ts, locks);
                    if (ms_before_expired > 0)
                    {
                        bo.backoffWithMaxSleep(
                            boTxnLock, ms_before_expired, Exception("2PC prewrite locked: " + std::to_string(locks.size()), LockError));
                    }
                    // retry
                    coroutines[i](i);
                    size_t *id = nullptr;
                    bool ok = false;
                    cq.Next((void **)&id, &ok);
                    assert(*id == i);
                    coroutines[i](i);
                    --i;
                    continue;
                }
                else
                {
                    if (batches[i].keys[0] == primary_lock)
                    {
                        // After writing the primary key, if the size of the transaction is large than 32M,
                        // start the ttlManager. The ttlManager will be closed in tikvTxn.Commit().
                        if (txn_size > ttlManagerRunThreshold)
                        {
                            ttl_manager.run(shared_from_this());
                        }
                    }
                }
            }
        }

        void BCOSTwoPhaseCommitter::rollbackSingleBatch(Backoffer &bo, const BatchKeys &batch)
        {
            auto req = std::make_shared<kvrpcpb::BatchRollbackRequest>();
            for (const auto &key : batch.keys)
            {
                req->add_keys(key);
            }
            req->set_start_version(start_ts);

            std::shared_ptr<kvrpcpb::BatchRollbackResponse> response;
            RegionClient region_client(cluster, batch.region);
            try
            {
                response = region_client.sendReqToRegion(bo, req);
            }
            catch (Exception &e)
            { // Region Error.
                std::cerr << "rollbackSingleBatch failed, " << e.what() << ":" << e.message() << std::endl;
                bo.backoff(boRegionMiss, e);
                rollbackKeys(bo, batch.keys);
                return;
            }
            if (response->has_error())
            {
                std::cerr << "rollbackSingleBatch failed, errors: " << response->error().ShortDebugString() << std::endl;
                throw Exception("meet errors: " + response->error().ShortDebugString(), LockError);
            }
        }

        void BCOSTwoPhaseCommitter::commitSingleBatch(Backoffer &bo, const BatchKeys &batch)
        {
            auto req = std::make_shared<kvrpcpb::CommitRequest>();
            for (const auto &key : batch.keys)
            {
                req->add_keys(key);
            }
            req->set_start_version(start_ts);
            req->set_commit_version(commit_ts);

            std::shared_ptr<kvrpcpb::CommitResponse> response;
            RegionClient region_client(cluster, batch.region);
            try
            {
                response = region_client.sendReqToRegion(bo, req);
            }
            catch (Exception &e)
            {
                std::cerr << "commitSingleBatch failed, " << e.what() << ":" << e.message() << std::endl;
                bo.backoff(boRegionMiss, e);
                commit_ts = cluster->pd_client->getTS();
                commitKeys(bo, batch.keys);
                return;
            }
            if (response->has_error())
            {
                std::cerr << "commitSingleBatch failed, errors: " << response->error().ShortDebugString() << std::endl;
                throw Exception("meet errors: " + response->error().ShortDebugString(), LockError);
            }

            commited = true;
        }

    } // namespace kv
} // namespace pingcap
