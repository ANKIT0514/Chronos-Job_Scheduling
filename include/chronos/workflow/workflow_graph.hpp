#pragma once

#include "chronos/domain/job.hpp"

#include <map>
#include <set>
#include <string>
#include <vector>

namespace chronos {

class WorkflowGraph {
public:
    void add_job(const Job& job);
    bool add_dependency(
        const std::string& job_id,
        const std::string& prerequisite_id);
    std::vector<std::vector<Job> > topological_levels() const;
    std::vector<Job> topological_order() const;

private:
    std::map<std::string, Job> jobs_;
    std::map<std::string, std::set<std::string> > prerequisites_;
};

}
