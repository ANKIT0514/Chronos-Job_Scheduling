#include "chronos/workflow/workflow_executor.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace chronos {
namespace {

Job make_job(const std::string& id) {
    Job job;
    job.id = id;
    return job;
}

TEST(WorkflowGraphTest, ProducesDeterministicTopologicalLevels) {
    WorkflowGraph graph;
    graph.add_job(make_job("root"));
    graph.add_job(make_job("left"));
    graph.add_job(make_job("right"));
    graph.add_job(make_job("finish"));
    ASSERT_TRUE(graph.add_dependency("left", "root"));
    ASSERT_TRUE(graph.add_dependency("right", "root"));
    ASSERT_TRUE(graph.add_dependency("finish", "left"));
    ASSERT_TRUE(graph.add_dependency("finish", "right"));
    EXPECT_FALSE(graph.add_dependency("finish", "right"));

    const std::vector<std::vector<Job> > levels = graph.topological_levels();
    ASSERT_EQ(levels.size(), 3U);
    ASSERT_EQ(levels[0].size(), 1U);
    EXPECT_EQ(levels[0][0].id, "root");
    ASSERT_EQ(levels[1].size(), 2U);
    EXPECT_EQ(levels[1][0].id, "left");
    EXPECT_EQ(levels[1][1].id, "right");
    ASSERT_EQ(levels[2].size(), 1U);
    EXPECT_EQ(levels[2][0].id, "finish");
}

TEST(WorkflowGraphTest, DetectsCyclesBeforeReturningAnOrder) {
    WorkflowGraph graph;
    graph.add_job(make_job("a"));
    graph.add_job(make_job("b"));
    graph.add_dependency("a", "b");
    graph.add_dependency("b", "a");

    EXPECT_THROW(graph.topological_order(), std::logic_error);
}

TEST(WorkflowGraphTest, RejectsInvalidJobsAndDependencies) {
    WorkflowGraph graph;
    EXPECT_THROW(graph.add_job(make_job("")), std::invalid_argument);
    graph.add_job(make_job("a"));
    EXPECT_THROW(graph.add_job(make_job("a")), std::invalid_argument);
    EXPECT_THROW(graph.add_dependency("a", "missing"), std::out_of_range);
    EXPECT_THROW(graph.add_dependency("a", "a"), std::invalid_argument);
}

TEST(WorkflowGraphTest, ExecutesJobsOnlyAfterTheirPrerequisites) {
    WorkflowGraph graph;
    graph.add_job(make_job("root"));
    graph.add_job(make_job("left"));
    graph.add_job(make_job("right"));
    graph.add_job(make_job("finish"));
    graph.add_dependency("left", "root");
    graph.add_dependency("right", "root");
    graph.add_dependency("finish", "left");
    graph.add_dependency("finish", "right");

    std::mutex mutex;
    std::vector<std::string> order;
    std::map<std::string, InMemoryScheduler::Task> tasks;
    const std::string ids[] = {"root", "left", "right", "finish"};
    for (std::size_t i = 0; i < 4; ++i) {
        tasks[ids[i]] = [&mutex, &order, id = ids[i]]() {
            std::lock_guard<std::mutex> lock(mutex);
            order.push_back(id);
        };
    }

    InMemoryScheduler scheduler(2);
    const WorkflowResult result = WorkflowExecutor::execute(graph, tasks, scheduler);

    EXPECT_TRUE(result.succeeded);
    ASSERT_EQ(result.jobs.size(), 4U);
    const std::vector<std::string>::const_iterator root =
        std::find(order.begin(), order.end(), "root");
    const std::vector<std::string>::const_iterator left =
        std::find(order.begin(), order.end(), "left");
    const std::vector<std::string>::const_iterator right =
        std::find(order.begin(), order.end(), "right");
    const std::vector<std::string>::const_iterator finish =
        std::find(order.begin(), order.end(), "finish");
    ASSERT_NE(root, order.end());
    ASSERT_NE(left, order.end());
    ASSERT_NE(right, order.end());
    ASSERT_NE(finish, order.end());
    EXPECT_LT(root, left);
    EXPECT_LT(root, right);
    EXPECT_LT(left, finish);
    EXPECT_LT(right, finish);
}

TEST(WorkflowGraphTest, StopsSubmittingDescendantsAfterFailure) {
    WorkflowGraph graph;
    graph.add_job(make_job("root"));
    graph.add_job(make_job("child"));
    graph.add_dependency("child", "root");
    std::map<std::string, InMemoryScheduler::Task> tasks;
    tasks["root"] = []() { throw std::runtime_error("injected failure"); };
    tasks["child"] = []() {};

    InMemoryScheduler scheduler(
        1, RetryPolicy(1, std::chrono::milliseconds(0),
                       std::chrono::milliseconds(0)));
    const WorkflowResult result = WorkflowExecutor::execute(graph, tasks, scheduler);

    EXPECT_FALSE(result.succeeded);
    EXPECT_EQ(result.failed_job_id, "root");
    EXPECT_EQ(result.jobs.size(), 1U);
    EXPECT_THROW(scheduler.status("child"), std::out_of_range);
}

TEST(WorkflowGraphTest, ValidatesAllTasksBeforeSchedulingAnyJob) {
    WorkflowGraph graph;
    graph.add_job(make_job("root"));
    graph.add_job(make_job("child"));
    graph.add_dependency("child", "root");
    std::map<std::string, InMemoryScheduler::Task> tasks;
    tasks["root"] = []() {};

    InMemoryScheduler scheduler(1);
    EXPECT_THROW(
        WorkflowExecutor::execute(graph, tasks, scheduler),
        std::invalid_argument);
    EXPECT_THROW(scheduler.status("root"), std::out_of_range);
}

}
}
