//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
  enum class LockMode : uint8_t { SHARED, EXCLUSIVE };

  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode)
        : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false), wounded_{false} {}

    bool operator==(const LockRequest &lr) const { return txn_id_ == lr.txn_id_ && lock_mode_ == lr.lock_mode_; };
    bool operator!=(const LockRequest &lr) const { return !operator==(lr); }

    txn_id_t txn_id_;
    LockMode lock_mode_;
    bool granted_;
    bool wounded_;
  };

  class LockRequestQueue {
   public:
    /**
     * @brief Check if the requesting lock mode is compatible with the existing ones.
     *
     * @param request the lock request
     * @return true if they are compatible, or false otherwise
     */
    bool RequestCompatible(LockRequest &request);

    std::list<LockRequest> grant_queue_;
    std::list<LockRequest> wait_queue_;
    // for notifying blocked transactions on this rid
    std::condition_variable cv_;
    // the mutex paired with cv_
    std::mutex mut_;
    // txn_id of an upgrading transaction (if any)
    txn_id_t upgrading_ = INVALID_TXN_ID;
  };

 public:
  /**
   * Creates a new lock manager configured for the deadlock prevention policy.
   */
  LockManager() = default;

  ~LockManager() = default;

  /*
   * [LOCK_NOTE]: For all locking functions, we:
   * 1. return false if the transaction is aborted; and
   * 2. block on wait, return true when the lock request is granted; and
   * 3. it is undefined behavior to try locking an already locked RID in the
   * same transaction, i.e. the transaction is responsible for keeping track of
   * its current locks.
   */

  /**
   * Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the shared lock
   * @param rid the RID to be locked in shared mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockShared(Transaction *txn, const RID &rid);

  /**
   * Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
   * @param txn the transaction requesting the exclusive lock
   * @param rid the RID to be locked in exclusive mode
   * @return true if the lock is granted, false otherwise
   */
  bool LockExclusive(Transaction *txn, const RID &rid);

  /**
   * Upgrade a lock from a shared lock to an exclusive lock.
   * @param txn the transaction requesting the lock upgrade
   * @param rid the RID that should already be locked in shared mode by the
   * requesting transaction
   * @return true if the upgrade is successful, false otherwise
   */
  bool LockUpgrade(Transaction *txn, const RID &rid);

  /**
   * Release the lock held by the transaction.
   * @param txn the transaction releasing the lock, it should actually hold the
   * lock
   * @param rid the RID that is locked by the transaction
   * @return true if the unlock is successful, false otherwise
   */
  bool Unlock(Transaction *txn, const RID &rid);

 private:
  /**
   * @brief Get the request queue based on the rid.
   *
   * @param rid the object identifier
   * @return LockRequestQueue* the pointer to the lock queue
   */
  LockRequestQueue *GetRequestQueue(const RID &rid);

  /**
   * @brief Check if the transaction is allowed to continue the lock operation.
   *
   * @param txn the transaction
   * @param mode the lock mode
   */
  void SanityCheck(Transaction *txn, LockMode mode);

  /**
   * Traverse the queue and try to wound suitable transactions
   *
   * @param queue the lock request queue (grant q or wait q)
   * @param txn_id the id of the transaction that executes the wounding
   * @return the number of wounded requests
   */
  int WoundRequestsInQueue(std::list<LockRequest> &queue, txn_id_t txn_id);

  /**
   * Given some requests and try to wound them.
   *
   * @param requests the whole lock requests queue
   * @param txn_id the id of the transaction that executes the wounding
   * @return the number of wounded requests
   */
  int TryWoundYoungerRequests(LockRequestQueue *requests, txn_id_t txn_id);

  /**
   * Enqueue a blocked lock request into the wait queue.
   *
   * @param requests the whole lock requests queue
   * @param txn the transaction
   * @param mode lock mode
   */
  void WaitInQueue(LockRequestQueue *requests, Transaction *txn, LockMode mode);

  std::mutex latch_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, std::unique_ptr<LockRequestQueue>> lock_table_;
};

}  // namespace bustub
