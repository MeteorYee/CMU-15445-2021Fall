//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

/** DistinctElement represents a key in a DISTINCT executor */
struct DistinctElement {
  /** The indexed attributes, namely the distinct attributes */
  std::vector<Value> distinct_key_;

  DistinctElement() = default;

  explicit DistinctElement(std::vector<Value> &&key) : distinct_key_{std::move(key)} {}

  ~DistinctElement() = default;

  /**
   * Compares two distinct keys for equality.
   * @param other the other distinct key to be compared with
   * @return `true` if both distinct keys have the same values of the attributes, `false` otherwise
   */
  bool operator==(const DistinctElement &other) const {
    for (uint32_t i = 0; i < other.distinct_key_.size(); i++) {
      if (distinct_key_[i].CompareEquals(other.distinct_key_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on DistinctElement */
template <>
struct hash<bustub::DistinctElement> {
  std::size_t operator()(const bustub::DistinctElement &distinct_element) const {
    size_t curr_hash = 0;
    for (const auto &key : distinct_element.distinct_key_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** The distinct key set */
  std::unordered_set<DistinctElement> distinct_set_;
  /** The set iterator */
  std::unordered_set<DistinctElement>::iterator set_it_;
};
}  // namespace bustub
