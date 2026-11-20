#include "chronos/scheduler/job_priority_queue.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <stdexcept>

namespace chronos {
namespace {

Job make_job(const std::string& id, int priority) {
    Job job;
    job.id = id;
    job.priority = priority;
    return job;
}

TEST(JobPriorityQueueTest, PopsJobsByDescendingPriority) {
    JobPriorityQueue queue;
    queue.push(make_job("low", 1));
    queue.push(make_job("high", 9));
    queue.push(make_job("medium", 5));

    EXPECT_EQ(queue.pop().id, "high");
    EXPECT_EQ(queue.pop().id, "medium");
    EXPECT_EQ(queue.pop().id, "low");
    EXPECT_TRUE(queue.empty());
}

TEST(JobPriorityQueueTest, PreservesFifoOrderForEqualPriorityAndDeadline) {
    JobPriorityQueue queue;
    queue.push(make_job("first", 3));
    queue.push(make_job("second", 3));

    EXPECT_EQ(queue.pop().id, "first");
    EXPECT_EQ(queue.pop().id, "second");
}

TEST(JobPriorityQueueTest, UsesEarlierDeadlineAfterPriority) {
    JobPriorityQueue queue;
    const std::chrono::system_clock::time_point now =
        std::chrono::system_clock::now();
    Job later = make_job("later", 3);
    later.has_deadline = true;
    later.deadline = now + std::chrono::hours(2);
    Job sooner = make_job("sooner", 3);
    sooner.has_deadline = true;
    sooner.deadline = now + std::chrono::hours(1);
    queue.push(later);
    queue.push(sooner);

    EXPECT_EQ(queue.pop().id, "sooner");
    EXPECT_EQ(queue.pop().id, "later");
}

TEST(JobPriorityQueueTest, ThrowsWhenReadingOrRemovingFromEmptyQueue) {
    JobPriorityQueue queue;

    EXPECT_THROW(queue.pop(), std::out_of_range);
    EXPECT_THROW(queue.top(), std::out_of_range);
}

TEST(JobPriorityQueueTest, ReportsSizeAndTopWithoutRemovingJob) {
    JobPriorityQueue queue;
    queue.push(make_job("first", 2));
    queue.push(make_job("second", 1));

    EXPECT_EQ(queue.size(), 2U);
    EXPECT_EQ(queue.top().id, "first");
    EXPECT_EQ(queue.size(), 2U);
}

}
}
