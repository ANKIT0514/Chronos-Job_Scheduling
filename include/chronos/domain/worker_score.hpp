#pragma once

#include <string>

namespace chronos {

struct WorkerScore {
    std::string worker_id;
    double score;
    bool eligible;

    WorkerScore() : score(0.0), eligible(false) {}
    WorkerScore(const std::string& id, double value, bool can_run = true)
        : worker_id(id), score(value), eligible(can_run) {}
};

}
