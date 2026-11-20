#include "chronos/persistence/redis_worker_store.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

namespace chronos {
namespace {

class RedisWorkerStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        const char* host = std::getenv("CHRONOS_TEST_REDIS_HOST");
        const char* port = std::getenv("CHRONOS_TEST_REDIS_PORT");
        if (host == NULL || port == NULL) {
            GTEST_SKIP() << "Set CHRONOS_TEST_REDIS_HOST and "
                            "CHRONOS_TEST_REDIS_PORT for Redis integration tests.";
        }
        store_.reset(new RedisWorkerStore(host, std::stoi(port)));
        worker_id_ = "redis-test-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
    }

    std::unique_ptr<RedisWorkerStore> store_;
    std::string worker_id_;
};

Worker make_worker(const std::string& id) {
    Worker worker;
    worker.id = id;
    worker.capacity.cpu_millis = 2000;
    worker.capacity.memory_bytes = 4096;
    worker.capacity.gpu_count = 1;
    worker.allocated.cpu_millis = 500;
    worker.allocated.memory_bytes = 1024;
    return worker;
}

TEST_F(RedisWorkerStoreTest, StoresExpiringWorkerStateAndAssignments) {
    const std::chrono::milliseconds ttl(3000);
    const Worker worker = make_worker(worker_id_);

    store_->store_heartbeat(worker, ttl);
    store_->store_load(worker, ttl);
    EXPECT_TRUE(store_->worker_is_alive(worker_id_));
    EXPECT_TRUE(store_->add_job(worker_id_, "job-1", ttl));
    EXPECT_TRUE(store_->has_job(worker_id_, "job-1"));
    EXPECT_TRUE(store_->remove_job(worker_id_, "job-1"));
    EXPECT_FALSE(store_->has_job(worker_id_, "job-1"));
}

TEST_F(RedisWorkerStoreTest, LeasesAreExclusiveAndTokenProtected) {
    const std::chrono::milliseconds ttl(1000);
    ASSERT_TRUE(store_->acquire_lease(worker_id_, "owner-a", ttl));
    EXPECT_FALSE(store_->acquire_lease(worker_id_, "owner-b", ttl));
    EXPECT_FALSE(store_->refresh_lease(worker_id_, "owner-b", ttl));
    EXPECT_TRUE(store_->refresh_lease(worker_id_, "owner-a", ttl));
    EXPECT_FALSE(store_->release_lease(worker_id_, "owner-b"));
    EXPECT_TRUE(store_->release_lease(worker_id_, "owner-a"));
    EXPECT_FALSE(store_->release_lease(worker_id_, "owner-a"));
}

TEST_F(RedisWorkerStoreTest, HeartbeatExpiresAndInputIsValidated) {
    store_->store_heartbeat(make_worker(worker_id_), std::chrono::milliseconds(30));
    EXPECT_TRUE(store_->worker_is_alive(worker_id_));
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    EXPECT_FALSE(store_->worker_is_alive(worker_id_));

    EXPECT_THROW(store_->store_heartbeat(
        make_worker("invalid worker"), std::chrono::seconds(1)),
        std::invalid_argument);
    EXPECT_THROW(store_->store_heartbeat(
        make_worker(worker_id_), std::chrono::milliseconds(0)),
        std::invalid_argument);
}

}
}
