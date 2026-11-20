#pragma once

#include "chronos/scheduler/in_memory_scheduler.hpp"
#include "chronos/workflow/workflow_graph.hpp"

#include <map>
#include <string>

namespace chronos {

struct WorkflowResult {
    bool succeeded;
    std::string failed_job_id;
    std::vector<JobStatus> jobs;

    WorkflowResult() : succeeded(true) {}
};

class WorkflowExecutor {
public:
    static WorkflowResult execute(
        const WorkflowGraph& graph,
        const std::map<std::string, InMemoryScheduler::Task>& tasks,
        InMemoryScheduler& scheduler);
};

}
