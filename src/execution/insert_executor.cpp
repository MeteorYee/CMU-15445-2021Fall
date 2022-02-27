//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  BUSTUB_ASSERT(table_info_ != Catalog::NULL_TABLE_INFO, "got an unknown table from the execution plan");
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  } else {
    raw_it_ = plan_->RawValues().begin();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple temp_tuple;
  RID temp_rid;
  if (!InnerNext(&temp_tuple)) {
    // if there is no more tuple to be inserted
    return false;
  }
  if (!table_info_->table_->InsertTuple(temp_tuple, &temp_rid, exec_ctx_->GetTransaction())) {
    // The insertion failed and the transaction is aborted.
    throw Exception(ExceptionType::INVALID, "Failed to execute the insertion.");
  }
  *rid = temp_rid;
  // insert into the indexes if applies, it will skip the for loop if there is no indexes at all
  for (const auto index : indexes_) {
    index->index_->InsertEntry(
        temp_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), temp_rid,
        exec_ctx_->GetTransaction());
  }
  return true;
}

bool InsertExecutor::InnerNext(Tuple *tuple) {
  if (child_executor_) {
    RID no_use_rid;
    return child_executor_->Next(tuple, &no_use_rid);
  }
  if (raw_it_ == plan_->RawValues().end()) {
    return false;
  }
  *tuple = Tuple(*raw_it_, &table_info_->schema_);
  ++raw_it_;
  return true;
}
}  // namespace bustub
