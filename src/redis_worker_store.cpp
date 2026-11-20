#include "chronos/persistence/redis_worker_store.hpp"

#include <sys/time.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace chronos {
namespace {

std::string number_string(std::uint64_t number) {
    return std::to_string(number);
}

bool valid_id_char(char character) {
    return (character >= 'A' && character <= 'Z') ||
           (character >= 'a' && character <= 'z') ||
           (character >= '0' && character <= '9') ||
           character == '_' || character == '.' || character == '-';
}

}

RedisWorkerStore::RedisWorkerStore(
    const std::string& host,
    int port,
    const std::string& password)
    : context_(NULL, &redisFree) {
    if (host.empty() || port < 1 || port > 65535) {
        throw std::invalid_argument("Redis host or port is invalid.");
    }

    struct timeval timeout;
    timeout.tv_sec = 2;
    timeout.tv_usec = 0;
    context_.reset(redisConnectWithTimeout(host.c_str(), port, timeout));
    if (!context_) {
        throw std::runtime_error("Could not allocate a Redis connection.");
    }
    if (context_->err != 0) {
        const std::string error = context_->errstr;
        throw std::runtime_error("Could not connect to Redis: " + error);
    }
    if (!password.empty()) {
        redisReply* reply = execute(std::vector<std::string>{"AUTH", password});
        if (reply->type != REDIS_REPLY_STATUS ||
            std::string(reply->str, reply->len) != "OK") {
            freeReplyObject(reply);
            throw std::runtime_error("Redis authentication was rejected.");
        }
        freeReplyObject(reply);
    }
}

void RedisWorkerStore::store_heartbeat(
    const Worker& worker,
    std::chrono::milliseconds ttl) {
    validate_worker_id(worker.id);
    validate_ttl(ttl);
    const std::string script =
        "redis.call('HSET', KEYS[1], 'id', ARGV[1], 'cpu_capacity', ARGV[2], "
        "'memory_capacity', ARGV[3], 'gpu_capacity', ARGV[4], "
        "'status', ARGV[5], 'last_heartbeat_ms', ARGV[6]); "
        "redis.call('PEXPIRE', KEYS[1], ARGV[7]); return 1";
    const std::vector<std::string> arguments{
        "EVAL", script, "1", worker_key(worker.id, "info"), worker.id,
        number_string(worker.capacity.cpu_millis),
        number_string(worker.capacity.memory_bytes),
        number_string(worker.capacity.gpu_count),
        worker.healthy ? "healthy" : "unhealthy",
        number_string(static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count())),
        number_string(static_cast<std::uint64_t>(ttl.count()))
    };

    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = execute(arguments);
    freeReplyObject(reply);
}

void RedisWorkerStore::store_load(
    const Worker& worker,
    std::chrono::milliseconds ttl) {
    validate_worker_id(worker.id);
    validate_ttl(ttl);
    const std::string script =
        "redis.call('HSET', KEYS[1], 'used_cpu_millis', ARGV[1], "
        "'used_memory_bytes', ARGV[2], 'used_gpu_count', ARGV[3]); "
        "redis.call('PEXPIRE', KEYS[1], ARGV[4]); return 1";
    const std::vector<std::string> arguments{
        "EVAL", script, "1", worker_key(worker.id, "load"),
        number_string(worker.allocated.cpu_millis),
        number_string(worker.allocated.memory_bytes),
        number_string(worker.allocated.gpu_count),
        number_string(static_cast<std::uint64_t>(ttl.count()))
    };

    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = execute(arguments);
    freeReplyObject(reply);
}

bool RedisWorkerStore::worker_is_alive(const std::string& worker_id) {
    validate_worker_id(worker_id);
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = execute(
        std::vector<std::string>{"EXISTS", worker_key(worker_id, "info")});
    const bool alive = reply->integer == 1;
    freeReplyObject(reply);
    return alive;
}

bool RedisWorkerStore::add_job(
    const std::string& worker_id,
    const std::string& job_id,
    std::chrono::milliseconds ttl) {
    validate_worker_id(worker_id);
    validate_ttl(ttl);
    const std::string script =
        "redis.call('SADD', KEYS[1], ARGV[1]); "
        "redis.call('PEXPIRE', KEYS[1], ARGV[2]); return 1";
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = execute(std::vector<std::string>{
        "EVAL", script, "1", worker_key(worker_id, "jobs"), job_id,
        number_string(static_cast<std::uint64_t>(ttl.count()))
    });
    const bool added = reply->integer == 1;
    freeReplyObject(reply);
    return added;
}

bool RedisWorkerStore::has_job(
    const std::string& worker_id,
    const std::string& job_id) {
    validate_worker_id(worker_id);
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = execute(std::vector<std::string>{
        "SISMEMBER", worker_key(worker_id, "jobs"), job_id
    });
    const bool found = reply->integer == 1;
    freeReplyObject(reply);
    return found;
}

bool RedisWorkerStore::remove_job(
    const std::string& worker_id,
    const std::string& job_id) {
    validate_worker_id(worker_id);
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = execute(std::vector<std::string>{
        "SREM", worker_key(worker_id, "jobs"), job_id
    });
    const bool removed = reply->integer == 1;
    freeReplyObject(reply);
    return removed;
}

bool RedisWorkerStore::acquire_lease(
    const std::string& worker_id,
    const std::string& token,
    std::chrono::milliseconds ttl) {
    validate_worker_id(worker_id);
    validate_ttl(ttl);
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = execute(std::vector<std::string>{
        "SET", worker_key(worker_id, "lease"), token, "NX", "PX",
        number_string(static_cast<std::uint64_t>(ttl.count()))
    });
    const bool acquired =
        reply->type == REDIS_REPLY_STATUS &&
        std::string(reply->str, reply->len) == "OK";
    freeReplyObject(reply);
    return acquired;
}

bool RedisWorkerStore::refresh_lease(
    const std::string& worker_id,
    const std::string& token,
    std::chrono::milliseconds ttl) {
    validate_worker_id(worker_id);
    validate_ttl(ttl);
    const std::string script =
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end";
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = execute(std::vector<std::string>{
        "EVAL", script, "1", worker_key(worker_id, "lease"), token,
        number_string(static_cast<std::uint64_t>(ttl.count()))
    });
    const bool refreshed = reply->integer == 1;
    freeReplyObject(reply);
    return refreshed;
}

bool RedisWorkerStore::release_lease(
    const std::string& worker_id,
    const std::string& token) {
    validate_worker_id(worker_id);
    const std::string script =
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "return redis.call('DEL', KEYS[1]) else return 0 end";
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = execute(std::vector<std::string>{
        "EVAL", script, "1", worker_key(worker_id, "lease"), token
    });
    const bool released = reply->integer == 1;
    freeReplyObject(reply);
    return released;
}

redisReply* RedisWorkerStore::execute(
    const std::vector<std::string>& arguments) {
    std::vector<const char*> argv;
    std::vector<std::size_t> lengths;
    argv.reserve(arguments.size());
    lengths.reserve(arguments.size());
    for (std::vector<std::string>::const_iterator it = arguments.begin();
         it != arguments.end(); ++it) {
        argv.push_back(it->data());
        lengths.push_back(it->size());
    }

    redisReply* reply = static_cast<redisReply*>(redisCommandArgv(
        context_.get(), static_cast<int>(argv.size()),
        argv.data(), lengths.data()));
    if (reply == NULL) {
        throw std::runtime_error(
            std::string("Redis command failed: ") + context_->errstr);
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        const std::string error(reply->str, reply->len);
        freeReplyObject(reply);
        throw std::runtime_error("Redis command rejected: " + error);
    }
    return reply;
}

std::string RedisWorkerStore::worker_key(
    const std::string& worker_id,
    const std::string& suffix) {
    return "worker:{" + worker_id + "}:" + suffix;
}

void RedisWorkerStore::validate_worker_id(const std::string& worker_id) {
    if (worker_id.empty() || worker_id.size() > 64) {
        throw std::invalid_argument("Worker ID length is invalid.");
    }
    for (std::string::const_iterator it = worker_id.begin();
         it != worker_id.end(); ++it) {
        if (!valid_id_char(*it)) {
            throw std::invalid_argument("Worker ID contains an invalid character.");
        }
    }
}

void RedisWorkerStore::validate_ttl(std::chrono::milliseconds ttl) {
    if (ttl.count() <= 0) {
        throw std::invalid_argument("Redis key TTL must be positive.");
    }
}

}
