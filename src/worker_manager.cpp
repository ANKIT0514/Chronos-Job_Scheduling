#include "chronos/worker/worker_manager.hpp"

#include <stdexcept>
#include <utility>

namespace chronos {

WorkerManager::WorkerManager(std::chrono::milliseconds heartbeat_timeout)
    : heartbeat_timeout_(heartbeat_timeout) {
    if (heartbeat_timeout_.count() <= 0) {
        throw std::invalid_argument(
            "The worker heartbeat timeout must be positive.");
    }
}

bool WorkerManager::register_worker(
    const Worker& worker,
    std::chrono::steady_clock::time_point now) {
    if (worker.id.empty()) {
        throw std::invalid_argument("A worker must have a non-empty ID.");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const std::map<std::string, WorkerRecord>::iterator existing =
        workers_.find(worker.id);
    if (existing != workers_.end()) {
        if (existing->second.worker.healthy) {
            return false;
        }
        WorkerRecord record;
        record.worker = worker;
        record.worker.healthy = true;
        record.last_heartbeat = now;
        existing->second = record;
        return true;
    }
    WorkerRecord record;
    record.worker = worker;
    record.worker.healthy = true;
    record.last_heartbeat = now;
    workers_.insert(std::make_pair(worker.id, record));
    return true;
}

void WorkerManager::heartbeat(
    const std::string& worker_id,
    std::chrono::steady_clock::time_point now) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::map<std::string, WorkerRecord>::iterator it =
        workers_.find(worker_id);
    if (it == workers_.end()) {
        throw std::out_of_range("Unknown worker ID: " + worker_id);
    }
    if (!it->second.worker.healthy) {
        throw std::logic_error(
            "A timed-out worker must register again before heartbeating.");
    }
    it->second.last_heartbeat = now;
}

bool WorkerManager::assign_job(const std::string& worker_id, const Job& job) {
    if (job.id.empty()) {
        throw std::invalid_argument("A job must have a non-empty ID.");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const std::map<std::string, WorkerRecord>::iterator it =
        workers_.find(worker_id);
    if (it == workers_.end()) {
        throw std::out_of_range("Unknown worker ID: " + worker_id);
    }
    if (!it->second.worker.healthy ||
        job_owners_.find(job.id) != job_owners_.end() ||
        !has_capacity(it->second.worker, job)) {
        return false;
    }

    WorkerRecord& record = it->second;
    record.worker.allocated.cpu_millis += job.resources.cpu_millis;
    record.worker.allocated.memory_bytes += job.resources.memory_bytes;
    record.worker.allocated.gpu_count += job.resources.gpu_count;
    record.jobs.insert(std::make_pair(job.id, job));
    job_owners_.insert(std::make_pair(job.id, worker_id));
    return true;
}

bool WorkerManager::complete_job(
    const std::string& worker_id,
    const std::string& job_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::map<std::string, std::string>::iterator owner =
        job_owners_.find(job_id);
    if (owner == job_owners_.end()) {
        return false;
    }
    if (owner->second != worker_id) {
        throw std::logic_error("The job is assigned to a different worker.");
    }

    WorkerRecord& record = workers_.find(worker_id)->second;
    const Job& job = record.jobs.find(job_id)->second;
    record.worker.allocated.cpu_millis -= job.resources.cpu_millis;
    record.worker.allocated.memory_bytes -= job.resources.memory_bytes;
    record.worker.allocated.gpu_count -= job.resources.gpu_count;
    record.jobs.erase(job_id);
    job_owners_.erase(owner);
    return true;
}

std::vector<WorkerFailure> WorkerManager::detect_timeouts(
    std::chrono::steady_clock::time_point now) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<WorkerFailure> failures;
    for (std::map<std::string, WorkerRecord>::iterator it = workers_.begin();
         it != workers_.end(); ++it) {
        WorkerRecord& record = it->second;
        if (!record.worker.healthy ||
            now - record.last_heartbeat < heartbeat_timeout_) {
            continue;
        }

        record.worker.healthy = false;
        WorkerFailure failure;
        failure.worker_id = it->first;
        for (std::map<std::string, Job>::const_iterator job = record.jobs.begin();
             job != record.jobs.end(); ++job) {
            failure.lost_jobs.push_back(job->second);
            job_owners_.erase(job->first);
        }
        record.jobs.clear();
        record.worker.allocated = ResourceRequest();
        failures.push_back(failure);
    }
    return failures;
}

std::vector<Worker> WorkerManager::healthy_workers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Worker> result;
    for (std::map<std::string, WorkerRecord>::const_iterator it =
             workers_.begin();
         it != workers_.end(); ++it) {
        if (it->second.worker.healthy) {
            result.push_back(it->second.worker);
        }
    }
    return result;
}

bool WorkerManager::has_capacity(const Worker& worker, const Job& job) {
    const ResourceRequest& capacity = worker.capacity;
    const ResourceRequest& allocated = worker.allocated;
    const ResourceRequest& requested = job.resources;
    return allocated.cpu_millis <= capacity.cpu_millis &&
           allocated.memory_bytes <= capacity.memory_bytes &&
           allocated.gpu_count <= capacity.gpu_count &&
           requested.cpu_millis <= capacity.cpu_millis - allocated.cpu_millis &&
           requested.memory_bytes <=
               capacity.memory_bytes - allocated.memory_bytes &&
           requested.gpu_count <= capacity.gpu_count - allocated.gpu_count;
}

}
