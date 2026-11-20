#pragma once

#include "chronos/domain/job.hpp"

#include <cstddef>
#include <queue>
#include <vector>

namespace chronos {

class JobPriorityQueue {
public:
    void push(const Job& job);
    Job pop();
    const Job& top() const;
    bool empty() const;
    std::size_t size() const;

private:
    struct Entry {
        Job job;
        std::size_t sequence;
    };

    struct LowerPriority {
        bool operator()(const Entry& left, const Entry& right) const;
    };

    std::priority_queue<Entry, std::vector<Entry>, LowerPriority> jobs_;
    std::size_t next_sequence_ = 0;
};

}
