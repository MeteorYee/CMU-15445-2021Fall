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

#include <atomic>
#include <thread>

#include "common/logger.h"
#include "common/macros.h"

namespace bustub {
class SpinLock {
 private:
  static constexpr int MAX_TRY_TIMES = 10;

  std::atomic_flag mLock_ = ATOMIC_FLAG_INIT;
  long counter = 0;
  long lock_counter = 0;

 public:
  SpinLock() = default;
  DISALLOW_COPY(SpinLock);

  void Lock() noexcept {
    int try_count = 1;
    int yield_time = 0;
    while (mLock_.test_and_set(std::memory_order_acquire)) {
      if (try_count == MAX_TRY_TIMES) {
        try_count = 0;
        yield_time++;
        std::this_thread::yield();
      }
      try_count++;
    }
    counter += yield_time * MAX_TRY_TIMES + try_count;
    lock_counter++;
  }

  void Unlock() noexcept { mLock_.clear(std::memory_order_release); }

  void PrintStats() {
    LOG_INFO("counter = %ld, lock_counter = %ld, c/lc = %.2f", counter, lock_counter, counter * 1.0 / lock_counter);
  }
};
}  // namespace bustub