//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {

LockManager::LockRequestQueue *LockManager::GetRequestQueue(const RID &rid) {
  std::scoped_lock lock(latch_);
  if (lock_table_.count(rid) == 0) {
    lock_table_.emplace(rid, std::make_unique<LockRequestQueue>());
  }
  return lock_table_[rid].get();
}

bool LockManager::LockRequestQueue::RequestCompatible(LockRequest &request) {
  if (grant_queue_.empty()) {
    // the request is the first one coming
    return true;
  }
  if (request.lock_mode_ == LockMode::EXCLUSIVE) {
    return false;
  }
  BUSTUB_ASSERT(request.lock_mode_ == LockMode::SHARED, "The lock mode must be SHARED.");
  LockRequest &last_request = grant_queue_.back();
  if (last_request.lock_mode_ == LockMode::SHARED && last_request.granted_) {
    return true;
  }
  return false;
}

void LockManager::SanityCheck(Transaction *txn, LockMode mode) {
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  if (mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
}

int LockManager::WoundRequestsInQueue(std::list<LockRequest> &queue, txn_id_t txn_id) {
  int wound_count = 0;
  for (auto &req : queue) {
    if (req.txn_id_ == txn_id) {
      // the request won't wound any requests waiting behind itself
      break;
    }
    if (!req.wounded_ && req.txn_id_ > txn_id) {
      // the younger transaction should be wounded
      Transaction *young_txn = TransactionManager::GetTransaction(req.txn_id_);
      req.wounded_ = true;
      young_txn->SetState(TransactionState::ABORTED);
      ++wound_count;
    }
  }
  return wound_count;
}

int LockManager::TryWoundYoungerRequests(LockRequestQueue *requests, txn_id_t txn_id) {
  (void)WoundRequestsInQueue(requests->grant_queue_, txn_id);
  // we only care about the number of waiting requests that are wounded
  return WoundRequestsInQueue(requests->wait_queue_, txn_id);
}

void LockManager::WaitInQueue(LockRequestQueue *requests, Transaction *txn, LockMode mode) {
  txn_id_t txn_id = txn->GetTransactionId();
  LockRequest lock_request{txn_id, mode};

  std::unique_lock q_lock(requests->mut_);
  requests->wait_queue_.emplace_back(txn_id, mode);

  while (requests->wait_queue_.front() != lock_request || !requests->RequestCompatible(lock_request)) {
    if (TryWoundYoungerRequests(requests, txn_id) > 0) {
      // notify the wounded ones to abort themselves
      requests->cv_.notify_all();
    }
    /* Although we might have wounded some transations, but we still need to wait them to
     * release the lock. */
    requests->cv_.wait(q_lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      // it's been wounded during the waiting
      requests->wait_queue_.remove(lock_request);
      q_lock.unlock();
      throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
    }
  }
  requests->wait_queue_.pop_front();
  lock_request.granted_ = true;
  requests->grant_queue_.push_back(std::move(lock_request));
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  SanityCheck(txn, LockMode::SHARED);
  if (txn->GetSharedLockSet()->count(rid) || txn->GetExclusiveLockSet()->count(rid)) {
    // the lock is re-entring
    return true;
  }
  WaitInQueue(GetRequestQueue(rid), txn, LockMode::SHARED);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  SanityCheck(txn, LockMode::EXCLUSIVE);
  if (txn->GetExclusiveLockSet()->count(rid)) {
    // the lock is re-entring
    return true;
  }
  WaitInQueue(GetRequestQueue(rid), txn, LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  SanityCheck(txn, LockMode::EXCLUSIVE);
  if (txn->GetExclusiveLockSet()->count(rid)) {
    // the lock is re-entring
    return true;
  }

  // try to remove the shared lock and upgrade
  LockRequestQueue *requests = GetRequestQueue(rid);
  {
    std::unique_lock q_lock(requests->mut_);
    if (requests->upgrading_ != INVALID_TXN_ID) {
      q_lock.unlock();
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    }

    requests->upgrading_ = txn->GetTransactionId();
    std::list<LockRequest>::iterator it = requests->grant_queue_.begin();
    bool has_found{false};
    while (it != requests->grant_queue_.end()) {
      if (it->txn_id_ == txn->GetTransactionId()) {
        BUSTUB_ASSERT(it->lock_mode_ == LockMode::SHARED, "The lock mode must be shared.");
        BUSTUB_ASSERT(it->granted_, "The request must have been granted.");
        requests->grant_queue_.erase(it);
        has_found = true;
        break;
      }
      ++it;
    }
    BUSTUB_ASSERT(has_found, "The upgrading lock request must exist at the first place.");
  }

  // re-enter the wait queue
  WaitInQueue(requests, txn, LockMode::EXCLUSIVE);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  bool has_found{false};
  bool is_shared_mode{false};
  LockRequestQueue *requests = GetRequestQueue(rid);
  {
    std::unique_lock q_lock(requests->mut_);
    std::list<LockRequest>::iterator it = requests->grant_queue_.begin();
    while (it != requests->grant_queue_.end()) {
      if (it->txn_id_ != txn->GetTransactionId()) {
        ++it;
        continue;
      }
      BUSTUB_ASSERT(it->granted_, "Ungranted requests shouldn't be found in unlock.");
      if (it->lock_mode_ == LockMode::SHARED) {
        txn->GetSharedLockSet()->erase(rid);
        is_shared_mode = true;
      } else {
        txn->GetExclusiveLockSet()->erase(rid);
      }
      requests->grant_queue_.erase(it);
      if (requests->grant_queue_.empty()) {
        // notify the waiters
        requests->cv_.notify_all();
      }
      has_found = true;
      break;
    }
  }

  if (txn->GetState() == TransactionState::GROWING) {
    if (!(is_shared_mode && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  if (has_found) {
    return true;
  } else {
    LOG_WARN("Didn't find the request specified in unlock.");
    return false;
  }
}

}  // namespace bustub
