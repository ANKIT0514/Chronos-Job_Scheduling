#include "chronos/scheduler/in_memory_scheduler.hpp"

#include <stdexcept>
#include <utility>

namespace chronos {

InMemoryScheduler::InMemoryScheduler(
    std::size_t worker_count,
    const RetryPolicy& retry_policy)
    : retry_policy_(retry_policy), stopping_(false) {
    if (worker_count == 0) {
        throw std::invalid_argument("The scheduler needs at least one worker.");
    }

    try {
        for (std::size_t i = 0; i < worker_count; ++i) {
            workers_.push_back(std::thread(&InMemoryScheduler::worker_loop, this));
        }
        retry_worker_ = std::thread(&InMemoryScheduler::retry_loop, this);
    } catch (...) {
        shutdown();
        throw;
    }
}

InMemoryScheduler::~InMemoryScheduler() {
    shutdown();
}

bool InMemoryScheduler::submit(const Job& job, const Task& task) {
    if (job.id.empty()) {
        throw std::invalid_argument("A job must have a non-empty ID.");
    }
    if (!task) {
        throw std::invalid_argument("A job must have an executable task.");
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_ || jobs_.find(job.id) != jobs_.end()) {
            return false;
        }
        std::shared_ptr<JobRecord> record(new JobRecord(job, task));
        jobs_.insert(std::make_pair(job.id, record));
        queue_.push(job);
    }
    work_available_.notify_one();
    return true;
}

bool InMemoryScheduler::cancel(const std::string& job_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::shared_ptr<JobRecord> record = find_record_locked(job_id);
    if (record->state == JobState::Pending) {
        record->state = JobState::Cancelled;
    } else if (record->state == JobState::Running) {
        record->cancellation_requested = true;
    } else if (record->state == JobState::Retrying) {
        record->state = JobState::Cancelled;
    } else {
        return false;
    }
    state_changed_.notify_all();
    return true;
}

JobStatus InMemoryScheduler::status(const std::string& job_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::shared_ptr<JobRecord> record = find_record_locked(job_id);
    return JobStatus{job_id, record->state, record->error, record->attempts};
}

JobStatus InMemoryScheduler::wait(const std::string& job_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    const std::shared_ptr<JobRecord> record = find_record_locked(job_id);
    state_changed_.wait(lock, [&record]() {
        return record->state == JobState::Succeeded ||
               record->state == JobState::Failed ||
               record->state == JobState::DeadLetter ||
               record->state == JobState::Cancelled;
    });
    return JobStatus{job_id, record->state, record->error, record->attempts};
}

void InMemoryScheduler::worker_loop() {
    while (true) {
        std::shared_ptr<JobRecord> record;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_available_.wait(lock, [this]() {
                return stopping_ || !queue_.empty();
            });
            if (queue_.empty()) {
                if (stopping_) {
                    return;
                }
                continue;
            }

            const Job job = queue_.pop();
            const std::map<std::string, std::shared_ptr<JobRecord> >::iterator it =
                jobs_.find(job.id);
            if (it == jobs_.end() || it->second->state != JobState::Pending) {
                continue;
            }
            record = it->second;
            record->state = JobState::Running;
            state_changed_.notify_all();
        }

        bool failed = false;
        std::string error;
        try {
            record->task();
        } catch (const std::exception& exception) {
            failed = true;
            error = exception.what();
        } catch (...) {
            failed = true;
            error = "Unknown exception during job execution.";
        }

        bool retry_scheduled = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++record->attempts;
            record->error = error;
            if (record->cancellation_requested) {
                record->state = JobState::Cancelled;
            } else if (failed) {
                if (!stopping_ &&
                    retry_policy_.should_retry(record->attempts)) {
                    record->state = JobState::Retrying;
                    retries_.insert(std::make_pair(
                        std::chrono::steady_clock::now() +
                            retry_policy_.delay_after_attempt(record->attempts),
                        record));
                    retry_scheduled = true;
                } else {
                    record->state = JobState::DeadLetter;
                }
            } else {
                record->error.clear();
                record->state = JobState::Succeeded;
            }
        }
        state_changed_.notify_all();
        if (retry_scheduled) {
            retry_available_.notify_one();
        }
    }
}

void InMemoryScheduler::retry_loop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopping_) {
        if (retries_.empty()) {
            retry_available_.wait(lock, [this]() {
                return stopping_ || !retries_.empty();
            });
            continue;
        }

        const std::chrono::steady_clock::time_point next_retry =
            retries_.begin()->first;
        const bool interrupted = retry_available_.wait_until(
            lock, next_retry, [this, next_retry]() {
                return stopping_ || retries_.empty() ||
                       retries_.begin()->first < next_retry;
            });
        if (stopping_) {
            return;
        }
        if (interrupted || retries_.empty() ||
            retries_.begin()->first > std::chrono::steady_clock::now()) {
            continue;
        }

        const std::shared_ptr<JobRecord> record = retries_.begin()->second;
        retries_.erase(retries_.begin());
        if (record->state == JobState::Retrying) {
            record->state = JobState::Pending;
            queue_.push(record->job);
            state_changed_.notify_all();
            lock.unlock();
            work_available_.notify_one();
            lock.lock();
        }
    }
}

void InMemoryScheduler::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
        for (std::multimap<std::chrono::steady_clock::time_point,
                           std::shared_ptr<JobRecord> >::iterator it =
                 retries_.begin();
             it != retries_.end(); ++it) {
            if (it->second->state == JobState::Retrying) {
                it->second->state = JobState::Pending;
                queue_.push(it->second->job);
            }
        }
        retries_.clear();
    }
    retry_available_.notify_all();
    work_available_.notify_all();
    state_changed_.notify_all();
    if (retry_worker_.joinable()) {
        retry_worker_.join();
    }
    for (std::vector<std::thread>::iterator it = workers_.begin();
         it != workers_.end(); ++it) {
        if (it->joinable()) {
            it->join();
        }
    }
}

std::shared_ptr<InMemoryScheduler::JobRecord>
InMemoryScheduler::find_record_locked(const std::string& job_id) const {
    const std::map<std::string, std::shared_ptr<JobRecord> >::const_iterator it =
        jobs_.find(job_id);
    if (it == jobs_.end()) {
        throw std::out_of_range("Unknown job ID: " + job_id);
    }
    return it->second;
}

}
