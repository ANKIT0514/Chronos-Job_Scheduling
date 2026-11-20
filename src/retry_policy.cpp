#include "chronos/scheduler/retry_policy.hpp"

#include <algorithm>
#include <stdexcept>

namespace chronos {

RetryPolicy::RetryPolicy(
    std::size_t max_attempts,
    std::chrono::milliseconds initial_backoff,
    std::chrono::milliseconds max_backoff)
    : max_attempts_(max_attempts),
      initial_backoff_(initial_backoff),
      max_backoff_(max_backoff) {
    if (max_attempts_ == 0) {
        throw std::invalid_argument("A retry policy needs at least one attempt.");
    }
    if (initial_backoff_.count() < 0 ||
        max_backoff_ < initial_backoff_) {
        throw std::invalid_argument("Retry backoff bounds are invalid.");
    }
}

bool RetryPolicy::should_retry(std::size_t completed_attempts) const {
    return completed_attempts > 0 && completed_attempts < max_attempts_;
}

std::chrono::milliseconds RetryPolicy::delay_after_attempt(
    std::size_t completed_attempts) const {
    if (completed_attempts == 0) {
        throw std::invalid_argument(
            "Backoff is defined only after a completed attempt.");
    }
    if (initial_backoff_.count() == 0) {
        return initial_backoff_;
    }

    std::chrono::milliseconds delay = initial_backoff_;
    for (std::size_t attempt = 1;
         attempt < completed_attempts && delay < max_backoff_;
         ++attempt) {
        if (delay.count() > max_backoff_.count() / 2) {
            return max_backoff_;
        }
        delay *= 2;
    }
    return std::min(delay, max_backoff_);
}

std::size_t RetryPolicy::max_attempts() const {
    return max_attempts_;
}

}
