//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  BUSTUB_ASSERT(child_executor_ != nullptr, "the child executor is null in the delete executor");
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  BUSTUB_ASSERT(table_info_ != Catalog::NULL_TABLE_INFO, "got an unknown table from the execution plan");
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple temp_tuple;
  RID temp_rid;
  if (!child_executor_->Next(&temp_tuple, &temp_rid)) {
    // Found nothing to delete given the conditions
    return false;
  }
  Transaction *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    exec_ctx_->GetLockManager()->LockUpgrade(txn, temp_rid);
  } else {
    exec_ctx_->GetLockManager()->LockExclusive(txn, temp_rid);
  }

  if (!table_info_->table_->MarkDelete(temp_rid, exec_ctx_->GetTransaction())) {
    LOG_WARN("Found nothing to delete in the table, wrong page!");
    return false;
  }
  // revise the indexes accordingly, it will skip the for loop if there is no indexes at all
  for (const auto index : indexes_) {
    index->index_->DeleteEntry(
        temp_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs()), temp_rid, txn);
    txn->GetIndexWriteSet()->emplace_back(temp_rid, table_info_->oid_, WType::DELETE, temp_tuple, index->index_oid_,
                                          exec_ctx_->GetCatalog());
  }
  return true;
}

}  // namespace bustub
