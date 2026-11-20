#pragma once

#include "chronos/domain/job.hpp"
#include "chronos/domain/worker.hpp"

#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace chronos {

struct WorkerFailure {
    std::string worker_id;
    std::vector<Job> lost_jobs;
};

class WorkerManager {
public:
    explicit WorkerManager(std::chrono::milliseconds heartbeat_timeout);

    bool register_worker(
        const Worker& worker,
        std::chrono::steady_clock::time_point now =
            std::chrono::steady_clock::now());
    void heartbeat(
        const std::string& worker_id,
        std::chrono::steady_clock::time_point now =
            std::chrono::steady_clock::now());
    bool assign_job(const std::string& worker_id, const Job& job);
    bool complete_job(const std::string& worker_id, const std::string& job_id);
    std::vector<WorkerFailure> detect_timeouts(
        std::chrono::steady_clock::time_point now =
            std::chrono::steady_clock::now());
    std::vector<Worker> healthy_workers() const;

private:
    struct WorkerRecord {
        Worker worker;
        std::chrono::steady_clock::time_point last_heartbeat;
        std::map<std::string, Job> jobs;
    };

    static bool has_capacity(const Worker& worker, const Job& job);

    const std::chrono::milliseconds heartbeat_timeout_;
    mutable std::mutex mutex_;
    std::map<std::string, WorkerRecord> workers_;
    std::map<std::string, std::string> job_owners_;
};

}
