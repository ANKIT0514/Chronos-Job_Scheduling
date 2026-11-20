#include "chronos/scheduler/job_priority_queue.hpp"

#include <stdexcept>

namespace chronos {

void JobPriorityQueue::push(const Job& job) {
    jobs_.push(Entry{job, next_sequence_++});
}

Job JobPriorityQueue::pop() {
    if (jobs_.empty()) {
        throw std::out_of_range("Cannot pop an empty job priority queue.");
    }

    Job job = jobs_.top().job;
    jobs_.pop();
    return job;
}

const Job& JobPriorityQueue::top() const {
    if (jobs_.empty()) {
        throw std::out_of_range("Cannot read the top of an empty job priority queue.");
    }
    return jobs_.top().job;
}

bool JobPriorityQueue::empty() const {
    return jobs_.empty();
}

std::size_t JobPriorityQueue::size() const {
    return jobs_.size();
}

bool JobPriorityQueue::LowerPriority::operator()(
    const Entry& left,
    const Entry& right) const {
    if (left.job.priority != right.job.priority) {
        return left.job.priority < right.job.priority;
    }
    if (left.job.has_deadline != right.job.has_deadline) {
        return !left.job.has_deadline;
    }
    if (left.job.has_deadline && left.job.deadline != right.job.deadline) {
        return left.job.deadline > right.job.deadline;
    }
    return left.sequence > right.sequence;
}

}
