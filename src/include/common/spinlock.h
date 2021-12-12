//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// spinlock.h
//
// Identification: src/include/common/spinlock.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sched.h>
#include <atomic>

#include "common/logger.h"
#include "common/macros.h"

namespace bustub {
class SpinLock {
 private:
  static constexpr int MAX_TRY_TIMES = 10;

  std::atomic_flag mlock_ = ATOMIC_FLAG_INIT;
#ifdef SPIN_LOCK_DEBUG
  uint64_t counter_ = 0;
  uint64_t lock_counter_ = 0;
#endif

 public:
  SpinLock() = default;
  DISALLOW_COPY(SpinLock);

  void Lock() noexcept {
    int try_count = 1;
    while (mlock_.test_and_set(std::memory_order_acquire)) {
      if (try_count == MAX_TRY_TIMES) {
        try_count = 0;
#ifdef SPIN_LOCK_DEBUG
        counter_ += MAX_TRY_TIMES;
#endif
        sched_yield();
      }
      try_count++;
    }
#ifdef SPIN_LOCK_DEBUG
    counter_ += try_count;
    lock_counter_++;
#endif
  }

  void Unlock() noexcept { mlock_.clear(std::memory_order_release); }

#ifdef SPIN_LOCK_DEBUG
  void PrintStats() {
    LOG_INFO("counter = %ld, lock_counter = %ld, c/lc = %.2f", counter_, lock_counter_, counter_ * 1.0 / lock_counter_);
  }
#endif
};
}  // namespace bustub
