//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      left_child_{std::move(left_child)},
      right_child_{std::move(right_child)},
      has_next_{false},
      hit_value_{nullptr} {
  output_schema_ = plan_->OutputSchema();
  BUSTUB_ASSERT(output_schema_ != nullptr, "The output shouldn't be null");
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  // build up the partition for the outer relation completely in memory
  Tuple tuple;
  RID rid;
  while (left_child_->Next(&tuple, &rid)) {
    Value value = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_child_->GetOutputSchema());
    HashJoinKey key{value};
    hj_ht_.Insert(key, tuple);
  }
  has_next_ = true;
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!right_tuple_.IsAllocated()) {
    has_next_ = InnerNext(rid);
  }

  // probe the inner relation
  while (has_next_) {
    if (tuple_it_ == hit_value_->tuples_.end()) {
      // the current right tuple has been completely tested
      has_next_ = InnerNext(rid);
      continue;
    }
    Tuple &left_tuple = *tuple_it_;
    ++tuple_it_;
    if (!JoinPredicate(&left_tuple)) {
      continue;
    }

    std::vector<Value> output_values;
    output_values.reserve(output_schema_->GetColumnCount());
    // collect values based on the output schema
    for (const auto &col : output_schema_->GetColumns()) {
      output_values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_child_->GetOutputSchema(), &right_tuple_,
                                                          right_child_->GetOutputSchema()));
    }
    *tuple = Tuple(output_values, output_schema_);
    // the joined result doesn't have a specified rid
    return true;
  }
  return false;
}

bool HashJoinExecutor::InnerNext(RID *rid) {
  while (right_child_->Next(&right_tuple_, rid)) {
    hit_value_ = GetValueFromTuple(&right_tuple_);
    if (hit_value_ == nullptr) {
      continue;
    }
    tuple_it_ = hit_value_->tuples_.begin();
    return true;
  }
  return false;
}

HashJoinValue *HashJoinExecutor::GetValueFromTuple(Tuple *tuple) {
  Value tuple_value = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, right_child_->GetOutputSchema());
  HashJoinKey key{tuple_value};
  return hj_ht_.Get(key);
}

bool HashJoinExecutor::JoinPredicate(Tuple *left_tuple) {
  Value left_key_value = plan_->LeftJoinKeyExpression()->Evaluate(left_tuple, left_child_->GetOutputSchema());
  Value right_key_value = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, right_child_->GetOutputSchema());
  return left_key_value.CompareEquals(right_key_value) == CmpBool::CmpTrue;
}

}  // namespace bustub
