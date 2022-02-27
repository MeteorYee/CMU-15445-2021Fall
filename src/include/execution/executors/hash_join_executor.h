//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/** HashJoinKey represents a key in the hash index built on the outer relation of hash join */
struct HashJoinKey {
  /** The indexed attributes, namely the join attributes */
  Value key_value_;

  HashJoinKey() = default;

  explicit HashJoinKey(const Value &key) : key_value_{key} {}

  ~HashJoinKey() = default;

  /**
   * Compares two hash-join keys for equality.
   * @param other the other hash-join key to be compared with
   * @return `true` if both hash-join keys have the same values of the attributes, `false` otherwise
   */
  bool operator==(const HashJoinKey &other) const {
    return key_value_.CompareEquals(other.key_value_) == CmpBool::CmpTrue;
  }
};

/** HashJoinValue represents a value for each hash join key */
struct HashJoinValue {
  /** All the tuples with the same key */
  std::vector<Tuple> tuples_;

  HashJoinValue() = default;

  explicit HashJoinValue(std::vector<Tuple> &&tuples) : tuples_{std::move(tuples)} {}

  ~HashJoinValue() = default;
};

}  // namespace bustub

namespace std {
/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashJoinKey> {
  std::size_t operator()(const bustub::HashJoinKey &hash_join_key) const {
    return bustub::HashUtil::HashValue(&hash_join_key.key_value_);
  }
};

}  // namespace std

namespace bustub {

class HashJoinHashTable {
 public:
  HashJoinValue *Get(const HashJoinKey &key) {
    if (ht_.count(key) == 0) {
      return nullptr;
    }
    return &ht_[key];
  }

  void Insert(const HashJoinKey &key, const Tuple &tuple) {
    HashJoinValue value(std::vector<Tuple>{tuple});
    if (ht_.count(key) == 0) {
      ht_.emplace(key, value);
    } else {
      ht_[key].tuples_.push_back(tuple);
    }
  }

 private:
  std::unordered_map<HashJoinKey, HashJoinValue> ht_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /**
   * @brief the inner logic of one iteration of the executor
   *
   * @param rid a in fact no-use parameter in accordance with the logical process of `Next(tuple, rid)`
   * @return true if there is still tuples to go, or false otherwise
   */
  bool InnerNext(RID *rid);

  /**
   * @brief Get the HashJoinValue given the specified tuple.
   *
   * @param tuple the tuple whose corresponding value will be returned
   * @return HashJoinValue* the value in the hash join table
   */
  HashJoinValue *GetValueFromTuple(Tuple *tuple);

  /**
   * @brief Check if the join predicate is met
   *
   * @param left_tuple the left tuple whose key will be tested against the `right_tuple_`
   * @return true if the join condition is met, or false otherwise
   */
  bool JoinPredicate(Tuple *left_tuple);

  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The schema of output results */
  const Schema *output_schema_;

  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  bool has_next_;
  Tuple right_tuple_;
  HashJoinValue *hit_value_;
  std::vector<Tuple>::iterator tuple_it_;

  HashJoinHashTable hj_ht_;
};

}  // namespace bustub
