//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_{std::move(child)} {
  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  auto group_by_exprs = plan_->GetGroupBys();
  auto aggregate_exprs = plan_->GetAggregates();
  while (child_->Next(&tuple, &rid)) {
    AggregateKey key;
    for (auto expr : group_by_exprs) {
      key.group_bys_.push_back(expr->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    AggregateValue value;
    for (auto expr : aggregate_exprs) {
      value.aggregates_.push_back(expr->Evaluate(&tuple, child_->GetOutputSchema()));
    }
    aht_->InsertCombine(key, value);
  }

  aht_iterator_ = aht_->Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_->End()) {
    const AbstractExpression *having = plan_->GetHaving();
    if (having != nullptr &&
        !having->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_).GetAs<bool>()) {
      ++aht_iterator_;
      continue;
    }
    const Schema *output_schema = plan_->OutputSchema();
    std::vector<Value> values;
    values.reserve(output_schema->GetColumnCount());
    // collect values based on the output schema
    for (const auto &col : output_schema->GetColumns()) {
      values.push_back(
          col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
    }
    *tuple = Tuple(values, output_schema);
    // no need to care about the rid here
    ++aht_iterator_;
    return true;
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
