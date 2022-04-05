//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the sequential scan */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /**
   * The routine after we fetch a tuple.
   * @param rid the tuple's rid
   */
  void TupleEntry(RID rid);

  /**
   * The routine before we return a tuple.
   * @param rid the tuple's rid
   */
  void TupleExit(RID rid);

  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  /** The table info to be scanned */
  const TableInfo *table_info_;
  /** The schema of output results */
  const Schema *output_schema_;

  /** The iterator of the table to be scanned */
  TableIterator tbit_{nullptr, RID(INVALID_PAGE_ID, 0), nullptr};
};
}  // namespace bustub
