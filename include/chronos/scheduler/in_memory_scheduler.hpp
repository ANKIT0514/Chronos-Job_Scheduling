#pragma once

#include "chronos/domain/job_state.hpp"
#include "chronos/domain/job.hpp"
#include "chronos/scheduler/job_priority_queue.hpp"
#include "chronos/scheduler/retry_policy.hpp"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace chronos {

struct JobStatus {
    std::string job_id;
    JobState state;
    std::string error;
    std::size_t attempts;
};

class InMemoryScheduler {
public:
    typedef std::function<void()> Task;

    explicit InMemoryScheduler(
        std::size_t worker_count,
        const RetryPolicy& retry_policy = RetryPolicy());
    ~InMemoryScheduler();

    InMemoryScheduler(const InMemoryScheduler&) = delete;
    InMemoryScheduler& operator=(const InMemoryScheduler&) = delete;

    bool submit(const Job& job, const Task& task);
    bool cancel(const std::string& job_id);
    JobStatus status(const std::string& job_id) const;
    JobStatus wait(const std::string& job_id);

private:
    struct JobRecord {
        Job job;
        Task task;
        JobState state;
        std::string error;
        std::size_t attempts;
        bool cancellation_requested;

        JobRecord(const Job& job_value, const Task& task_value)
            : job(job_value),
              task(task_value),
              state(JobState::Pending),
              attempts(0),
              cancellation_requested(false) {}
    };

    void worker_loop();
    void retry_loop();
    void shutdown();
    std::shared_ptr<JobRecord> find_record_locked(
        const std::string& job_id) const;

    mutable std::mutex mutex_;
    std::condition_variable work_available_;
    std::condition_variable state_changed_;
    std::condition_variable retry_available_;
    RetryPolicy retry_policy_;
    JobPriorityQueue queue_;
    std::map<std::string, std::shared_ptr<JobRecord> > jobs_;
    std::multimap<std::chrono::steady_clock::time_point,
                  std::shared_ptr<JobRecord> > retries_;
    std::vector<std::thread> workers_;
    std::thread retry_worker_;
    bool stopping_;
};

}
