#include "chronos/persistence/job_repository.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>

namespace chronos {
namespace {

std::string test_job_id() {
    const std::chrono::system_clock::duration::rep ticks =
        std::chrono::system_clock::now().time_since_epoch().count();
    return "postgres-test-" + std::to_string(ticks);
}

class PostgresRepositoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        const char* connection_string =
            std::getenv("CHRONOS_TEST_DATABASE_URL");
        if (connection_string == NULL || *connection_string == '\0') {
            GTEST_SKIP() << "Set CHRONOS_TEST_DATABASE_URL to run PostgreSQL integration tests.";
        }
        repository_.reset(new JobRepository(connection_string));
        repository_->apply_migration(
            std::string(CHRONOS_SOURCE_DIR) +
            "/sql/001_initial_schema.sql");
    }

    std::unique_ptr<JobRepository> repository_;
};

TEST_F(PostgresRepositoryTest, PersistsJobsAndIdempotentTransitions) {
    Job job;
    job.id = test_job_id();
    job.priority = 72;
    job.resources.cpu_millis = 500;
    job.resources.memory_bytes = 8ULL * 1024 * 1024 * 1024;
    job.resources.gpu_count = 1;
    job.has_deadline = true;
    job.deadline = std::chrono::system_clock::now() +
                   std::chrono::hours(1);

    ASSERT_TRUE(repository_->create_job(job));
    EXPECT_FALSE(repository_->create_job(job));
    EXPECT_EQ(repository_->transition_job(
        job.id, "event-pending-" + job.id, 0, JobState::Pending),
        TransitionResult::Applied);
    EXPECT_EQ(repository_->transition_job(
        job.id, "event-pending-" + job.id, 0, JobState::Pending),
        TransitionResult::Duplicate);
    EXPECT_EQ(repository_->transition_job(
        job.id, "event-scheduled-" + job.id, 1, JobState::Scheduled),
        TransitionResult::Applied);
    Worker worker;
    worker.id = "worker-test-" + job.id;
    worker.capacity.cpu_millis = 2000;
    worker.capacity.memory_bytes = 16ULL * 1024 * 1024 * 1024;
    worker.capacity.gpu_count = 2;
    repository_->upsert_worker(worker);
    EXPECT_EQ(repository_->transition_job(
        job.id, "event-running-" + job.id, 2, JobState::Running,
        std::string(), worker.id),
        TransitionResult::Applied);

    PersistentJob stored;
    ASSERT_TRUE(repository_->get_job(job.id, stored));
    EXPECT_EQ(stored.job.priority, 72);
    EXPECT_EQ(stored.job.resources.memory_bytes, job.resources.memory_bytes);
    EXPECT_TRUE(stored.job.has_deadline);
    EXPECT_EQ(stored.state, JobState::Running);
    EXPECT_EQ(stored.version, 3U);
    EXPECT_EQ(stored.attempts, 1U);
}

TEST_F(PostgresRepositoryTest, RejectsStaleAndIllegalTransitions) {
    Job job;
    job.id = test_job_id();
    ASSERT_TRUE(repository_->create_job(job));

    EXPECT_EQ(repository_->transition_job(
        job.id, "stale-" + job.id, 2, JobState::Pending),
        TransitionResult::Conflict);
    EXPECT_EQ(repository_->transition_job(
        job.id, "illegal-" + job.id, 0, JobState::Running),
        TransitionResult::InvalidTransition);
    EXPECT_EQ(repository_->transition_job(
        job.id, "used-" + job.id, 0, JobState::Pending),
        TransitionResult::Applied);
    EXPECT_EQ(repository_->transition_job(
        job.id, "used-" + job.id, 0, JobState::Succeeded),
        TransitionResult::IdempotencyConflict);
    PersistentJob absent;
    EXPECT_FALSE(repository_->get_job("absent-" + job.id, absent));
}

TEST_F(PostgresRepositoryTest, UpsertsWorkerStateAndHeartbeat) {
    Worker worker;
    worker.id = test_job_id();
    worker.capacity.cpu_millis = 4000;
    worker.capacity.memory_bytes = 16ULL * 1024 * 1024 * 1024;
    worker.capacity.gpu_count = 2;
    const std::chrono::system_clock::time_point heartbeat =
        std::chrono::system_clock::now();

    EXPECT_NO_THROW(repository_->upsert_worker(worker, heartbeat));
    worker.allocated.cpu_millis = 500;
    worker.healthy = false;
    EXPECT_NO_THROW(repository_->upsert_worker(
        worker, heartbeat + std::chrono::seconds(1)));
}

}
}
