//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    std::vector<Value> distinct_key;
    for (uint32_t col_idx = 0; col_idx < child_executor_->GetOutputSchema()->GetColumnCount(); ++col_idx) {
      distinct_key.push_back(tuple.GetValue(child_executor_->GetOutputSchema(), col_idx));
    }
    distinct_set_.emplace(std::move(distinct_key));
  }

  set_it_ = distinct_set_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (set_it_ == distinct_set_.end()) {
    return false;
  }
  *tuple = Tuple(set_it_->distinct_key_, plan_->OutputSchema());
  // no need to set the rid here
  ++set_it_;
  return true;
}

}  // namespace bustub
