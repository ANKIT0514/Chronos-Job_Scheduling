#include "chronos/scheduler/worker_selector.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <stdexcept>
#include <string>
#include <vector>

namespace chronos {
namespace {

Worker make_worker(
    const std::string& id,
    std::uint32_t cpu,
    std::uint64_t memory,
    std::uint32_t gpu = 0) {
    Worker worker;
    worker.id = id;
    worker.capacity.cpu_millis = cpu;
    worker.capacity.memory_bytes = memory;
    worker.capacity.gpu_count = gpu;
    return worker;
}

Job make_job(std::uint32_t cpu, std::uint64_t memory, std::uint32_t gpu = 0) {
    Job job;
    job.resources.cpu_millis = cpu;
    job.resources.memory_bytes = memory;
    job.resources.gpu_count = gpu;
    return job;
}

std::vector<Worker> make_conflicting_workers() {
    Worker tight = make_worker("tight", 1000, 4096);
    tight.allocated.cpu_millis = 400;
    Worker roomy = make_worker("roomy", 4000, 16384);
    return std::vector<Worker>{tight, roomy};
}

TEST(WorkerSelectorTest, LeastLoadedPrefersLowerCurrentUtilization) {
    const Job job = make_job(500, 1024);
    const WorkerScore selected = select_worker(
        job, make_conflicting_workers(), SchedulingStrategy::LeastLoaded);

    EXPECT_EQ(selected.worker_id, "roomy");
    EXPECT_TRUE(selected.eligible);
}

TEST(WorkerSelectorTest, ResourceAwarePacksIntoSmallestSufficientWorker) {
    const Job job = make_job(500, 1024);
    const WorkerScore selected = select_worker(
        job, make_conflicting_workers(), SchedulingStrategy::ResourceAware);

    EXPECT_EQ(selected.worker_id, "tight");
}

TEST(WorkerSelectorTest, PriorityChangesTheWorkerScoringTradeoff) {
    std::vector<Worker> workers = make_conflicting_workers();
    Job urgent = make_job(500, 1024);
    urgent.priority = 100;
    Job normal = urgent;
    normal.priority = 0;

    EXPECT_EQ(select_worker(urgent, workers, SchedulingStrategy::PriorityBased)
                  .worker_id,
              "roomy");
    EXPECT_EQ(select_worker(normal, workers, SchedulingStrategy::PriorityBased)
                  .worker_id,
              "tight");
}

TEST(WorkerSelectorTest, DeadlineAwarePrefersSoonestExpectedCompletion) {
    const std::chrono::system_clock::time_point now =
        std::chrono::system_clock::now();
    Job job = make_job(500, 1024);
    job.estimated_runtime_ms = 300;
    job.has_deadline = true;
    job.deadline = now + std::chrono::milliseconds(1000);
    Worker slow = make_worker("slow", 1000, 4096);
    slow.estimated_start_delay_ms = 800;
    Worker fast = make_worker("fast", 1000, 4096);
    fast.estimated_start_delay_ms = 100;

    EXPECT_EQ(select_worker(
                  job, std::vector<Worker>{slow, fast},
                  SchedulingStrategy::DeadlineAware, now).worker_id,
              "fast");
}

TEST(WorkerSelectorTest, RejectsUnhealthyAndResourceInsufficientWorkers) {
    Job job = make_job(500, 1024, 1);
    Worker unhealthy = make_worker("unhealthy", 1000, 4096, 1);
    unhealthy.healthy = false;
    Worker no_gpu = make_worker("no-gpu", 1000, 4096, 0);
    const std::vector<WorkerScore> scores = rank_workers(
        job, std::vector<Worker>{unhealthy, no_gpu},
        SchedulingStrategy::ResourceAware);

    ASSERT_EQ(scores.size(), 2U);
    EXPECT_FALSE(scores[0].eligible);
    EXPECT_FALSE(scores[1].eligible);
    EXPECT_THROW(select_worker(
        job, std::vector<Worker>{unhealthy, no_gpu},
        SchedulingStrategy::ResourceAware), std::runtime_error);
}

TEST(WorkerSelectorTest, RejectsOvercommittedWorker) {
    Job job = make_job(1, 0);
    Worker worker = make_worker("overcommitted", 1000, 0);
    worker.allocated.cpu_millis = 1001;

    EXPECT_THROW(select_worker(
        job, std::vector<Worker>{worker},
        SchedulingStrategy::LeastLoaded), std::runtime_error);
}

}
}
