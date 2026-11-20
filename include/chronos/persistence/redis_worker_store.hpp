#pragma once

#include "chronos/domain/worker.hpp"

#include <hiredis/hiredis.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace chronos {

class RedisWorkerStore {
public:
    RedisWorkerStore(
        const std::string& host,
        int port,
        const std::string& password = std::string());
    ~RedisWorkerStore() = default;

    RedisWorkerStore(const RedisWorkerStore&) = delete;
    RedisWorkerStore& operator=(const RedisWorkerStore&) = delete;

    void store_heartbeat(
        const Worker& worker,
        std::chrono::milliseconds ttl);
    void store_load(
        const Worker& worker,
        std::chrono::milliseconds ttl);
    bool worker_is_alive(const std::string& worker_id);
    bool add_job(
        const std::string& worker_id,
        const std::string& job_id,
        std::chrono::milliseconds ttl);
    bool has_job(const std::string& worker_id, const std::string& job_id);
    bool remove_job(const std::string& worker_id, const std::string& job_id);
    bool acquire_lease(
        const std::string& worker_id,
        const std::string& token,
        std::chrono::milliseconds ttl);
    bool refresh_lease(
        const std::string& worker_id,
        const std::string& token,
        std::chrono::milliseconds ttl);
    bool release_lease(const std::string& worker_id, const std::string& token);

private:
    redisReply* execute(const std::vector<std::string>& arguments);
    static std::string worker_key(
        const std::string& worker_id,
        const std::string& suffix);
    static void validate_worker_id(const std::string& worker_id);
    static void validate_ttl(std::chrono::milliseconds ttl);

    std::unique_ptr<redisContext, void (*)(redisContext*)> context_;
    std::mutex mutex_;
};

}
