#include "chronos/workflow/workflow_graph.hpp"

#include <stdexcept>
#include <utility>

namespace chronos {

void WorkflowGraph::add_job(const Job& job) {
    if (job.id.empty()) {
        throw std::invalid_argument("A workflow job must have a non-empty ID.");
    }
    if (!jobs_.insert(std::make_pair(job.id, job)).second) {
        throw std::invalid_argument("Duplicate workflow job ID: " + job.id);
    }
    prerequisites_.insert(
        std::make_pair(job.id, std::set<std::string>()));
}

bool WorkflowGraph::add_dependency(
    const std::string& job_id,
    const std::string& prerequisite_id) {
    if (job_id == prerequisite_id) {
        throw std::invalid_argument("A job cannot depend on itself.");
    }
    if (jobs_.find(job_id) == jobs_.end()) {
        throw std::out_of_range("Unknown dependent job ID: " + job_id);
    }
    if (jobs_.find(prerequisite_id) == jobs_.end()) {
        throw std::out_of_range(
            "Unknown prerequisite job ID: " + prerequisite_id);
    }
    return prerequisites_.find(job_id)->second.insert(prerequisite_id).second;
}

std::vector<std::vector<Job> > WorkflowGraph::topological_levels() const {
    std::map<std::string, std::size_t> indegree;
    std::map<std::string, std::set<std::string> > dependents;
    for (std::map<std::string, Job>::const_iterator job = jobs_.begin();
         job != jobs_.end(); ++job) {
        indegree.insert(std::make_pair(job->first, 0));
    }
    for (std::map<std::string, std::set<std::string> >::const_iterator job =
             prerequisites_.begin();
         job != prerequisites_.end(); ++job) {
        indegree[job->first] = job->second.size();
        for (std::set<std::string>::const_iterator prerequisite =
                 job->second.begin();
             prerequisite != job->second.end(); ++prerequisite) {
            dependents[*prerequisite].insert(job->first);
        }
    }

    std::set<std::string> ready;
    for (std::map<std::string, std::size_t>::const_iterator job =
             indegree.begin();
         job != indegree.end(); ++job) {
        if (job->second == 0) {
            ready.insert(job->first);
        }
    }

    std::vector<std::vector<Job> > levels;
    std::size_t visited = 0;
    while (!ready.empty()) {
        const std::set<std::string> current = ready;
        ready.clear();
        std::vector<Job> level;
        for (std::set<std::string>::const_iterator id = current.begin();
             id != current.end(); ++id) {
            level.push_back(jobs_.find(*id)->second);
            ++visited;
            const std::map<std::string, std::set<std::string> >::const_iterator
                downstream = dependents.find(*id);
            if (downstream == dependents.end()) {
                continue;
            }
            for (std::set<std::string>::const_iterator child =
                     downstream->second.begin();
                 child != downstream->second.end(); ++child) {
                std::size_t& child_indegree = indegree.find(*child)->second;
                --child_indegree;
                if (child_indegree == 0) {
                    ready.insert(*child);
                }
            }
        }
        levels.push_back(level);
    }

    if (visited != jobs_.size()) {
        throw std::logic_error("Workflow graph contains a dependency cycle.");
    }
    return levels;
}

std::vector<Job> WorkflowGraph::topological_order() const {
    const std::vector<std::vector<Job> > levels = topological_levels();
    std::vector<Job> result;
    result.reserve(jobs_.size());
    for (std::vector<std::vector<Job> >::const_iterator level = levels.begin();
         level != levels.end(); ++level) {
        result.insert(result.end(), level->begin(), level->end());
    }
    return result;
}

}
