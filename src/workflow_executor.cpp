#include "chronos/workflow/workflow_executor.hpp"

#include <stdexcept>

namespace chronos {

WorkflowResult WorkflowExecutor::execute(
    const WorkflowGraph& graph,
    const std::map<std::string, InMemoryScheduler::Task>& tasks,
    InMemoryScheduler& scheduler) {
    const std::vector<std::vector<Job> > levels = graph.topological_levels();
    for (std::vector<std::vector<Job> >::const_iterator level = levels.begin();
         level != levels.end(); ++level) {
        for (std::vector<Job>::const_iterator job = level->begin();
             job != level->end(); ++job) {
            const std::map<std::string, InMemoryScheduler::Task>::const_iterator
                task = tasks.find(job->id);
            if (task == tasks.end() || !task->second) {
                throw std::invalid_argument(
                    "Workflow job has no executable task: " + job->id);
            }
            try {
                scheduler.status(job->id);
                throw std::logic_error(
                    "Workflow job already exists in the scheduler: " + job->id);
            } catch (const std::out_of_range&) {
            }
        }
    }

    WorkflowResult result;
    for (std::vector<std::vector<Job> >::const_iterator level = levels.begin();
         level != levels.end(); ++level) {
        for (std::vector<Job>::const_iterator job = level->begin();
             job != level->end(); ++job) {
            if (!scheduler.submit(job[0], tasks.find(job->id)->second)) {
                throw std::runtime_error(
                    "The scheduler rejected workflow job: " + job->id);
            }
        }

        bool level_succeeded = true;
        for (std::vector<Job>::const_iterator job = level->begin();
             job != level->end(); ++job) {
            const JobStatus status = scheduler.wait(job->id);
            result.jobs.push_back(status);
            if (status.state != JobState::Succeeded && level_succeeded) {
                level_succeeded = false;
                result.failed_job_id = job->id;
            }
        }
        if (!level_succeeded) {
            result.succeeded = false;
            return result;
        }
    }
    return result;
}

}
