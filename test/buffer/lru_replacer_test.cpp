//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer_test.cpp
//
// Identification: test/buffer/lru_replacer_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/lru_replacer.h"
#include "common/logger.h"
#include "common/spinlock.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(LRUReplacerTest, OverflowTest) {
  LRUReplacer lru_replacer(2);

  // unpin(3) should fail
  lru_replacer.Unpin(0);
  lru_replacer.Unpin(1);
  lru_replacer.Unpin(3);

  EXPECT_EQ(2, lru_replacer.Size());
}

// NOLINTNEXTLINE
TEST(LRUReplacerTest, InvalidIDTest) {
  LRUReplacer lru_replacer(3);
  lru_replacer.Unpin(1);
  lru_replacer.Unpin(-1);
  lru_replacer.Unpin(3);

  EXPECT_EQ(1, lru_replacer.Size());

  lru_replacer.Pin(0);
  lru_replacer.Pin(-1);
  lru_replacer.Pin(3);
  lru_replacer.Pin(1);

  EXPECT_EQ(0, lru_replacer.Size());
}

// NOLINTNEXTLINE
TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer lru_replacer(7);

  // Scenario: unpin six elements, i.e. add them to the replacer.
  lru_replacer.Unpin(1);
  lru_replacer.Unpin(2);
  lru_replacer.Unpin(3);
  lru_replacer.Unpin(4);
  lru_replacer.Unpin(5);
  lru_replacer.Unpin(6);
  lru_replacer.Unpin(1);
  EXPECT_EQ(6, lru_replacer.Size());

  // Scenario: get three victims from the lru.
  int value;
  lru_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  // Scenario: pin elements in the replacer.
  // Note that 3 has already been victimized, so pinning 3 should have no effect.
  lru_replacer.Pin(3);
  lru_replacer.Pin(4);
  EXPECT_EQ(2, lru_replacer.Size());

  // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
  lru_replacer.Unpin(4);

  // Scenario: continue looking for victims. We expect these victims.
  lru_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(4, value);

  // find a victim from an empty lru, should have no effects
  lru_replacer.Victim(&value);
  EXPECT_EQ(4, value);
}

// NOLINTNEXTLINE
TEST(LRUReplacerTest, MultiThreadPinUnpinTest) {
  LRUReplacer lru_replacer(1024);

  auto unpin_func = [&lru_replacer](frame_id_t frame_id) {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    // NOLINTNEXTLINE
    LOG_INFO("Thread %s tries to unpin the frames starting from id = %u", ss.str().c_str(), frame_id);
    for (frame_id_t i = frame_id; i < frame_id + 256; i++) {
      lru_replacer.Unpin(i);
    }
  };

  std::thread th1(unpin_func, 0);
  std::thread th2(unpin_func, 256);
  std::thread th3(unpin_func, 512);
  std::thread th4(unpin_func, 768);

  th1.join();
  th2.join();
  th3.join();
  th4.join();

  EXPECT_EQ(1024, lru_replacer.Size());

  auto pin_func = [&lru_replacer](frame_id_t frame_id) {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    // NOLINTNEXTLINE
    LOG_INFO("Thread %s tries to pin the frames starting from id = %u", ss.str().c_str(), frame_id);
    for (frame_id_t i = frame_id; i < frame_id + 256; i++) {
      lru_replacer.Pin(i);
    }
  };

  std::thread th5(pin_func, 0);
  std::thread th6(pin_func, 256);
  std::thread th7(pin_func, 512);
  std::thread th8(pin_func, 768);

  th5.join();
  th6.join();
  th7.join();
  th8.join();

  EXPECT_EQ(0, lru_replacer.Size());
}

// NOLINTNEXTLINE
TEST(LRUReplacerTest, MultiThreadUnpinVictimTest) {
  LRUReplacer lru_replacer(1024);

  auto unpin_func = [&lru_replacer](frame_id_t frame_id) {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    // NOLINTNEXTLINE
    LOG_INFO("Thread %s tries to unpin the frames starting from id = %u", ss.str().c_str(), frame_id);
    for (frame_id_t i = frame_id; i < frame_id + 256; i++) {
      lru_replacer.Unpin(i);
    }
  };

  std::thread th1(unpin_func, 0);
  std::thread th2(unpin_func, 256);
  std::thread th3(unpin_func, 512);
  std::thread th4(unpin_func, 768);

  th1.join();
  th2.join();
  th3.join();
  th4.join();

  EXPECT_EQ(1024, lru_replacer.Size());

  auto victim_func = [&lru_replacer]() {
    std::stringstream ss;
    ss << std::this_thread::get_id();
    // NOLINTNEXTLINE
    LOG_INFO("Thread %s tries to victim 256 frames", ss.str().c_str());
    int value;
    for (frame_id_t i = 0; i < 256; i++) {
      lru_replacer.Victim(&value);
    }
  };

  std::thread th5(victim_func);
  std::thread th6(victim_func);
  std::thread th7(victim_func);
  std::thread th8(victim_func);

  th5.join();
  th6.join();
  th7.join();
  th8.join();

  EXPECT_EQ(0, lru_replacer.Size());
}

#ifdef SPIN_LOCK_DEBUG
// NOLINTNEXTLINE
TEST(MySpinLockTest, SampleTest) {
  SpinLock spin_lock;
  int counter = 0;
  auto func = [&spin_lock, &counter]() {
    for (int i = 0; i < 1000000; i++) {
      spin_lock.Lock();
      counter++;
      spin_lock.Unlock();
    }
  };

  std::thread th1(func);
  std::thread th2(func);

  th1.join();
  th2.join();

  LOG_INFO("The counter = %d", counter);
  spin_lock.PrintStats();
  EXPECT_EQ(2000000, counter);
}
#endif

}  // namespace bustub
