//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  BUSTUB_ASSERT(child_executor_ != nullptr, "the child executor is null in the delete executor");
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  BUSTUB_ASSERT(table_info_ != Catalog::NULL_TABLE_INFO, "got an unknown table from the execution plan");
  indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tuple;
  RID old_rid;
  if (!child_executor_->Next(&old_tuple, &old_rid)) {
    if (is_first_run_) {
      throw Exception(ExceptionType::INVALID, "Found nothing to update given the conditions.");
    }
    return false;
  }

  is_first_run_ = false;
  Tuple new_tuple = GenerateUpdatedTuple(old_tuple);
  RID new_rid = {old_rid.GetPageId(), old_rid.GetSlotNum()};
  TableHeap *table = table_info_->table_.get();
  Transaction *tx = exec_ctx_->GetTransaction();
  bool is_updated = table->UpdateTuple(new_tuple, old_rid, tx);
  bool is_delete_insert = false;
  if (!is_updated) {
    if (tx->GetState() == TransactionState::ABORTED) {
      throw Exception(ExceptionType::INVALID, "Failed to update the tuple");
    }
    // it's probably failed due to lack of space, hence we need to delete and insert
    if (!table->MarkDelete(old_rid, tx)) {
      throw Exception(ExceptionType::INVALID, "Failed to update the tuple");
    }
    if (!table->InsertTuple(new_tuple, &new_rid, tx)) {
      throw Exception(ExceptionType::INVALID, "Failed to update the tuple");
    }
    is_delete_insert = true;
  }
  // revise the indexes accordingly, it will skip the for loop if there is no indexes at all
  for (const auto index : indexes_) {
    std::vector<uint32_t> key_attrs = index->index_->GetKeyAttrs();
    if (!(is_delete_insert || NeedIndexUpdate(key_attrs))) {
      // if the updated attributes are not indexed and the tuple is not deleted followed by insertion,
      // then just skip the index
      continue;
    }
    const auto &old_key = old_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, key_attrs);
    const auto &new_key = new_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, key_attrs);
    index->index_->DeleteEntry(old_key, old_rid, tx);
    index->index_->InsertEntry(new_key, new_rid, tx);
  }
  return true;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

bool UpdateExecutor::NeedIndexUpdate(const std::vector<uint32_t> &key_attrs) const {
  const auto &update_attrs = plan_->GetUpdateAttr();
  for (const auto key_id : key_attrs) {
    if (update_attrs.find(key_id) != update_attrs.end()) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
