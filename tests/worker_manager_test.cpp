#include "chronos/worker/worker_manager.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <stdexcept>
#include <string>
#include <vector>

namespace chronos {
namespace {

Worker make_worker(const std::string& id) {
    Worker worker;
    worker.id = id;
    worker.capacity.cpu_millis = 2000;
    worker.capacity.memory_bytes = 4096;
    worker.capacity.gpu_count = 1;
    return worker;
}

Job make_job(const std::string& id) {
    Job job;
    job.id = id;
    job.resources.cpu_millis = 1000;
    job.resources.memory_bytes = 1024;
    job.resources.gpu_count = 1;
    return job;
}

TEST(WorkerManagerTest, DetectsHeartbeatTimeoutAndReassignsLostJobs) {
    const std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    WorkerManager manager(std::chrono::seconds(5));
    ASSERT_TRUE(manager.register_worker(make_worker("lost"), now));
    ASSERT_TRUE(manager.register_worker(make_worker("healthy"), now));
    ASSERT_TRUE(manager.assign_job("lost", make_job("job-1")));

    EXPECT_TRUE(manager.detect_timeouts(now + std::chrono::seconds(4)).empty());
    manager.heartbeat("healthy", now + std::chrono::seconds(4));
    const std::vector<WorkerFailure> failures =
        manager.detect_timeouts(now + std::chrono::seconds(5));

    ASSERT_EQ(failures.size(), 1U);
    std::size_t reassigned_jobs = 0;
    for (std::vector<WorkerFailure>::const_iterator it = failures.begin();
         it != failures.end(); ++it) {
        if (it->worker_id == "lost") {
            ASSERT_EQ(it->lost_jobs.size(), 1U);
            EXPECT_EQ(it->lost_jobs[0].id, "job-1");
            EXPECT_TRUE(manager.assign_job("healthy", it->lost_jobs[0]));
            ++reassigned_jobs;
        }
    }
    EXPECT_EQ(reassigned_jobs, 1U);
    ASSERT_EQ(manager.healthy_workers().size(), 1U);
    EXPECT_EQ(manager.healthy_workers()[0].id, "healthy");
}

TEST(WorkerManagerTest, HeartbeatExtendsTheWorkerLease) {
    const std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    WorkerManager manager(std::chrono::seconds(5));
    ASSERT_TRUE(manager.register_worker(make_worker("worker"), now));
    manager.heartbeat("worker", now + std::chrono::seconds(4));

    EXPECT_TRUE(manager.detect_timeouts(now + std::chrono::seconds(8)).empty());
    EXPECT_EQ(manager.detect_timeouts(now + std::chrono::seconds(9)).size(), 1U);
}

TEST(WorkerManagerTest, ReservationsPreventOvercommitAndCompletionReleasesIt) {
    WorkerManager manager(std::chrono::seconds(5));
    ASSERT_TRUE(manager.register_worker(make_worker("worker")));
    ASSERT_TRUE(manager.assign_job("worker", make_job("first")));
    EXPECT_FALSE(manager.assign_job("worker", make_job("second")));
    EXPECT_TRUE(manager.complete_job("worker", "first"));
    EXPECT_TRUE(manager.assign_job("worker", make_job("second")));
    EXPECT_FALSE(manager.complete_job("worker", "missing"));
}

TEST(WorkerManagerTest, DoesNotAllowTimedOutWorkerToHeartbeat) {
    const std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    WorkerManager manager(std::chrono::seconds(1));
    ASSERT_TRUE(manager.register_worker(make_worker("worker"), now));
    ASSERT_EQ(manager.detect_timeouts(now + std::chrono::seconds(1)).size(), 1U);

    EXPECT_THROW(manager.heartbeat("worker", now + std::chrono::seconds(2)),
                 std::logic_error);
    EXPECT_THROW(manager.heartbeat("missing", now), std::out_of_range);
    EXPECT_TRUE(manager.register_worker(make_worker("worker"),
                                        now + std::chrono::seconds(2)));
    EXPECT_NO_THROW(manager.heartbeat("worker", now + std::chrono::seconds(3)));
}

TEST(WorkerManagerTest, RejectsInvalidWorkersAndTimeouts) {
    EXPECT_THROW(WorkerManager(std::chrono::milliseconds(0)),
                 std::invalid_argument);
    WorkerManager manager(std::chrono::seconds(1));
    EXPECT_THROW(manager.register_worker(Worker()), std::invalid_argument);
    ASSERT_TRUE(manager.register_worker(make_worker("worker")));
    EXPECT_FALSE(manager.register_worker(make_worker("worker")));
}

}
}
