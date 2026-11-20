#include "chronos/scheduler/worker_selector.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace chronos {
namespace {

bool fits(const ResourceRequest& requested, const Worker& worker) {
    const ResourceRequest& capacity = worker.capacity;
    const ResourceRequest& allocated = worker.allocated;
    if (allocated.cpu_millis > capacity.cpu_millis ||
        allocated.memory_bytes > capacity.memory_bytes ||
        allocated.gpu_count > capacity.gpu_count) {
        return false;
    }

    return requested.cpu_millis <= capacity.cpu_millis - allocated.cpu_millis &&
           requested.memory_bytes <=
               capacity.memory_bytes - allocated.memory_bytes &&
           requested.gpu_count <= capacity.gpu_count - allocated.gpu_count;
}

double utilization(const Worker& worker) {
    double total = 0.0;
    unsigned int dimensions = 0;
    if (worker.capacity.cpu_millis > 0) {
        total += static_cast<double>(worker.allocated.cpu_millis) /
                 worker.capacity.cpu_millis;
        ++dimensions;
    }
    if (worker.capacity.memory_bytes > 0) {
        total += static_cast<double>(worker.allocated.memory_bytes) /
                 worker.capacity.memory_bytes;
        ++dimensions;
    }
    if (worker.capacity.gpu_count > 0) {
        total += static_cast<double>(worker.allocated.gpu_count) /
                 worker.capacity.gpu_count;
        ++dimensions;
    }
    return dimensions == 0 ? 0.0 : total / dimensions;
}

double resource_fit(const ResourceRequest& requested, const Worker& worker) {
    double total = 0.0;
    double dominant = 0.0;
    unsigned int dimensions = 0;

    if (requested.cpu_millis > 0) {
        const double ratio = static_cast<double>(requested.cpu_millis) /
            (worker.capacity.cpu_millis - worker.allocated.cpu_millis);
        total += ratio;
        dominant = std::max(dominant, ratio);
        ++dimensions;
    }
    if (requested.memory_bytes > 0) {
        const double ratio = static_cast<double>(requested.memory_bytes) /
            (worker.capacity.memory_bytes - worker.allocated.memory_bytes);
        total += ratio;
        dominant = std::max(dominant, ratio);
        ++dimensions;
    }
    if (requested.gpu_count > 0) {
        const double ratio = static_cast<double>(requested.gpu_count) /
            (worker.capacity.gpu_count - worker.allocated.gpu_count);
        total += ratio;
        dominant = std::max(dominant, ratio);
        ++dimensions;
    }

    if (dimensions == 0) {
        return 1.0 - utilization(worker);
    }
    return 0.7 * dominant + 0.3 * (total / dimensions);
}

double score_worker(
    const Job& job,
    const Worker& worker,
    SchedulingStrategy strategy,
    std::chrono::system_clock::time_point now) {
    const double least_loaded = 1.0 - utilization(worker);
    const double fit = resource_fit(job.resources, worker);

    switch (strategy) {
        case SchedulingStrategy::LeastLoaded:
            return least_loaded;
        case SchedulingStrategy::ResourceAware:
            return fit;
        case SchedulingStrategy::PriorityBased: {
            const int clamped_priority =
                std::max(0, std::min(job.priority, 100));
            const double priority_weight = clamped_priority / 100.0;
            return priority_weight * least_loaded +
                   (1.0 - priority_weight) * fit;
        }
        case SchedulingStrategy::DeadlineAware: {
            const double completion_ms =
                static_cast<double>(worker.estimated_start_delay_ms) +
                static_cast<double>(job.estimated_runtime_ms);
            if (!job.has_deadline) {
                return -completion_ms;
            }
            const double remaining_ms =
                std::chrono::duration<double, std::milli>(
                    job.deadline - now).count();
            const double lateness_ms =
                std::max(0.0, completion_ms - remaining_ms);
            return -completion_ms - 4.0 * lateness_ms;
        }
    }
    throw std::invalid_argument("Unknown scheduling strategy.");
}

}

std::vector<WorkerScore> rank_workers(
    const Job& job,
    const std::vector<Worker>& workers,
    SchedulingStrategy strategy,
    std::chrono::system_clock::time_point now) {
    std::vector<WorkerScore> scores;
    scores.reserve(workers.size());
    for (std::vector<Worker>::const_iterator it = workers.begin();
         it != workers.end(); ++it) {
        const bool eligible = it->healthy && fits(job.resources, *it);
        const double score = eligible
            ? score_worker(job, *it, strategy, now)
            : 0.0;
        scores.push_back(WorkerScore(it->id, score, eligible));
    }

    std::stable_sort(scores.begin(), scores.end(),
        [](const WorkerScore& left, const WorkerScore& right) {
            if (left.eligible != right.eligible) {
                return left.eligible;
            }
            return left.score > right.score;
        });
    return scores;
}

WorkerScore select_worker(
    const Job& job,
    const std::vector<Worker>& workers,
    SchedulingStrategy strategy,
    std::chrono::system_clock::time_point now) {
    const std::vector<WorkerScore> scores =
        rank_workers(job, workers, strategy, now);
    if (scores.empty() || !scores.front().eligible) {
        throw std::runtime_error(
            "No healthy worker has sufficient resources for the job.");
    }
    return scores.front();
}

}
