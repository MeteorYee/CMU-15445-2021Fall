//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_executor_{std::move(left_executor)},
      right_executor_{std::move(right_executor)} {
  output_schema_ = plan_->OutputSchema();
  BUSTUB_ASSERT(output_schema_ != nullptr, "The output shouldn't be null");
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (!(left_tuple_.IsAllocated() || left_executor_->Next(&left_tuple_, &left_rid_))) {
      // the left relation is the outer table, hence if it's the end, the total process is done
      return false;
    }
    if (!right_executor_->Next(&right_tuple_, &right_rid_)) {
      // start over the iteration of the right table
      if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
        // it's the end of the left table
        return false;
      }
      right_executor_->Init();
      if (!right_executor_->Next(&right_tuple_, &right_rid_)) {
        // the right table is empty
        return false;
      }
    }
    // up to this point, we must have the tuples of both sides available
    if (!plan_->Predicate()
             ->EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(), &right_tuple_,
                            plan_->GetRightPlan()->OutputSchema())
             .GetAs<bool>()) {
      continue;
    }
    std::vector<Value> values;
    values.reserve(output_schema_->GetColumnCount());
    // collect values based on the output schema
    for (const auto &col : output_schema_->GetColumns()) {
      values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                                   right_executor_->GetOutputSchema()));
    }
    *tuple = Tuple(values, output_schema_);
    // the joined result doesn't have a specified rid
    return true;
  }
}

}  // namespace bustub
