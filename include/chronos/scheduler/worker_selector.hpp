#pragma once

#include "chronos/domain/job.hpp"
#include "chronos/domain/worker.hpp"
#include "chronos/domain/worker_score.hpp"

#include <chrono>
#include <vector>

namespace chronos {

enum class SchedulingStrategy {
    LeastLoaded,
    ResourceAware,
    PriorityBased,
    DeadlineAware
};

std::vector<WorkerScore> rank_workers(
    const Job& job,
    const std::vector<Worker>& workers,
    SchedulingStrategy strategy,
    std::chrono::system_clock::time_point now =
        std::chrono::system_clock::now());

WorkerScore select_worker(
    const Job& job,
    const std::vector<Worker>& workers,
    SchedulingStrategy strategy,
    std::chrono::system_clock::time_point now =
        std::chrono::system_clock::now());

}
