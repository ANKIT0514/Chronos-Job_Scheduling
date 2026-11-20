#include "chronos/scheduler/in_memory_scheduler.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace chronos {
namespace {

Job make_job(const std::string& id, int priority = 0) {
    Job job;
    job.id = id;
    job.priority = priority;
    return job;
}

class Gate {
public:
    Gate() : entered_(false), released_(false) {}

    void block() {
        std::unique_lock<std::mutex> lock(mutex_);
        entered_ = true;
        condition_.notify_all();
        condition_.wait(lock, [this]() { return released_; });
    }

    bool wait_until_entered() {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(
            lock, std::chrono::seconds(2), [this]() { return entered_; });
    }

    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        condition_.notify_all();
    }

    ~Gate() {
        release();
    }

private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool entered_;
    bool released_;
};

TEST(InMemorySchedulerTest, RunsSubmittedTaskAndReportsSuccess) {
    InMemoryScheduler scheduler(2);
    std::atomic<int> result(0);

    ASSERT_TRUE(scheduler.submit(make_job("one"), [&result]() { ++result; }));
    const JobStatus status = scheduler.wait("one");

    EXPECT_EQ(status.state, JobState::Succeeded);
    EXPECT_EQ(result.load(), 1);
}

TEST(InMemorySchedulerTest, PreservesPriorityForQueuedJobs) {
    InMemoryScheduler scheduler(1);
    Gate gate;
    std::mutex mutex;
    std::vector<std::string> order;

    ASSERT_TRUE(scheduler.submit(make_job("blocker"), [&]() {
        gate.block();
    }));
    ASSERT_TRUE(gate.wait_until_entered());

    ASSERT_TRUE(scheduler.submit(make_job("low", 1), [&]() {
        std::lock_guard<std::mutex> lock(mutex);
        order.push_back("low");
    }));
    ASSERT_TRUE(scheduler.submit(make_job("high", 9), [&]() {
        std::lock_guard<std::mutex> lock(mutex);
        order.push_back("high");
    }));
    gate.release();

    EXPECT_EQ(scheduler.wait("low").state, JobState::Succeeded);
    EXPECT_EQ(scheduler.wait("high").state, JobState::Succeeded);
    ASSERT_EQ(order.size(), 2U);
    EXPECT_EQ(order[0], "high");
    EXPECT_EQ(order[1], "low");
}

TEST(InMemorySchedulerTest, ContainsTaskFailureAndPreservesItsMessage) {
    InMemoryScheduler scheduler(
        1, RetryPolicy(1, std::chrono::milliseconds(0),
                       std::chrono::milliseconds(0)));

    ASSERT_TRUE(scheduler.submit(make_job("failure"), []() {
        throw std::runtime_error("task failed");
    }));
    const JobStatus status = scheduler.wait("failure");

    EXPECT_EQ(status.state, JobState::DeadLetter);
    EXPECT_EQ(status.error, "task failed");
    EXPECT_EQ(status.attempts, 1U);
}

TEST(InMemorySchedulerTest, RetriesWithBackoffAndCompletesAfterRecovery) {
    InMemoryScheduler scheduler(
        1, RetryPolicy(3, std::chrono::milliseconds(1),
                       std::chrono::milliseconds(2)));
    std::atomic<int> attempts(0);

    ASSERT_TRUE(scheduler.submit(make_job("recover"), [&attempts]() {
        const int attempt = ++attempts;
        if (attempt < 3) {
            throw std::runtime_error("temporary failure");
        }
    }));
    const JobStatus status = scheduler.wait("recover");

    EXPECT_EQ(status.state, JobState::Succeeded);
    EXPECT_EQ(status.attempts, 3U);
    EXPECT_EQ(attempts.load(), 3);
    EXPECT_TRUE(status.error.empty());
}

TEST(InMemorySchedulerTest, MovesExhaustedRetriesToDeadLetter) {
    InMemoryScheduler scheduler(
        1, RetryPolicy(2, std::chrono::milliseconds(1),
                       std::chrono::milliseconds(1)));
    std::atomic<int> attempts(0);

    ASSERT_TRUE(scheduler.submit(make_job("dead"), [&attempts]() {
        ++attempts;
        throw std::runtime_error("persistent failure");
    }));
    const JobStatus status = scheduler.wait("dead");

    EXPECT_EQ(status.state, JobState::DeadLetter);
    EXPECT_EQ(status.attempts, 2U);
    EXPECT_EQ(attempts.load(), 2);
    EXPECT_EQ(status.error, "persistent failure");
}

TEST(InMemorySchedulerTest, CancelsPendingTaskWithoutExecutingIt) {
    InMemoryScheduler scheduler(1);
    Gate gate;
    std::atomic<bool> cancelled_task_ran(false);

    ASSERT_TRUE(scheduler.submit(make_job("blocker"), [&]() { gate.block(); }));
    ASSERT_TRUE(gate.wait_until_entered());

    ASSERT_TRUE(scheduler.submit(make_job("cancelled"), [&]() {
        cancelled_task_ran = true;
    }));
    EXPECT_TRUE(scheduler.cancel("cancelled"));
    EXPECT_EQ(scheduler.wait("cancelled").state, JobState::Cancelled);

    gate.release();
    EXPECT_EQ(scheduler.wait("blocker").state, JobState::Succeeded);
    EXPECT_FALSE(cancelled_task_ran.load());
}

TEST(InMemorySchedulerTest, RejectsDuplicateIdsAndReportsUnknownJobs) {
    InMemoryScheduler scheduler(1);
    std::promise<void> release;
    std::future<void> gate = release.get_future();

    ASSERT_TRUE(scheduler.submit(make_job("same"), [&gate]() { gate.wait(); }));
    EXPECT_FALSE(scheduler.submit(make_job("same"), []() {}));
    EXPECT_THROW(scheduler.status("missing"), std::out_of_range);

    release.set_value();
    EXPECT_EQ(scheduler.wait("same").state, JobState::Succeeded);
}

TEST(InMemorySchedulerTest, RequiresWorkersAndValidJobs) {
    EXPECT_THROW(InMemoryScheduler(0), std::invalid_argument);

    InMemoryScheduler scheduler(1);
    EXPECT_THROW(scheduler.submit(make_job(""), []() {}), std::invalid_argument);
    EXPECT_THROW(scheduler.submit(make_job("empty-task"), InMemoryScheduler::Task()),
                 std::invalid_argument);
}

}
}
