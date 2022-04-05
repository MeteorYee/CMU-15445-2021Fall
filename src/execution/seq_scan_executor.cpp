//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_{plan} {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  BUSTUB_ASSERT(table_info_ != Catalog::NULL_TABLE_INFO, "got an unknown table from the execution plan");
  output_schema_ = plan_->OutputSchema();
  BUSTUB_ASSERT(output_schema_ != nullptr, "The output shouldn't be null");
}

void SeqScanExecutor::Init() { tbit_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  const AbstractExpression *pred = plan_->GetPredicate();

  while (tbit_ != table_info_->table_->End()) {
    TupleEntry(tbit_->GetRid());
    if (pred != nullptr && !pred->Evaluate(&(*tbit_), &table_info_->schema_).GetAs<bool>()) {
      TupleExit(tbit_->GetRid());
      ++tbit_;
      continue;
    }
    std::vector<Value> values;
    values.reserve(output_schema_->GetColumnCount());
    // collect values based on the output schema
    for (const auto &col : output_schema_->GetColumns()) {
      values.push_back(col.GetExpr()->Evaluate(&(*tbit_), &table_info_->schema_));
    }
    *tuple = Tuple(values, output_schema_);
    *rid = tbit_->GetRid();
    TupleExit(tbit_->GetRid());
    ++tbit_;
    return true;
  }
  return false;
}

void SeqScanExecutor::TupleEntry(RID rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    return;
  }
  exec_ctx_->GetLockManager()->LockShared(txn, rid);
}

void SeqScanExecutor::TupleExit(RID rid) {
  Transaction *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    exec_ctx_->GetLockManager()->Unlock(txn, rid);
  }
}

}  // namespace bustub
