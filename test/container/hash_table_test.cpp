//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>  // NOLINT
#include <ctime>
#include <random>
#include <sstream>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, SplitInsertAndMergeTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // based on the size of HashTableBucketPage<int, int, IntComparator>
  const int max_elements_per_bucket = 496;

  // insert 5X max_elements_per_bucket which must cause at least two splits
  const int factor = 5;
  int dummy_key = 0;
  int dummy_value = 0;
  for (int k = 0; k < factor; k++) {
    for (int i = 0; i < max_elements_per_bucket; i++) {
      ht.Insert(nullptr, dummy_key, dummy_value);
      dummy_key++;
      dummy_value++;
    }
    ht.VerifyIntegrityAndPrint((k + 1) * max_elements_per_bucket, true);
  }

  // remove everthing again
  for (int k = 0; k < factor; k++) {
    for (int i = 0; i < max_elements_per_bucket; i++) {
      dummy_key--;
      dummy_value--;
      ASSERT_TRUE(ht.Remove(nullptr, dummy_key, dummy_value));
    }
    ht.VerifyIntegrityAndPrint((factor - k - 1) * max_elements_per_bucket, true);
  }

  // the hash table should go back to its initial form
  ASSERT_EQ(0, ht.GetGlobalDepth());

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, DirectoryPageFullTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(256, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // based on the size of HashTableBucketPage<int, int, IntComparator>
  const int max_elements_per_bucket = 496;
  // a number of elements that the extendible hash table can't consume
  const int elements_num = max_elements_per_bucket * DIRECTORY_ARRAY_SIZE + 1;

  int key = 0;
  int value = 0;
  for (int i = 0; i < elements_num; i++) {
    if (!ht.Insert(nullptr, key, value)) {
      // the directory page is full right now
      LOG_INFO("%d k-v pairs have been inserted", i);
      break;
    }
    if ((i + 1) % 30000 == 0) {
      LOG_INFO("%d k-v pairs have been inserted", (i + 1));
      ht.VerifyIntegrity();
    }
    key++;
    value++;
  }
  // no more k-v pairs can be inserted into the hash table
  ht.VerifyIntegrity();

  const int size = key;
  // remove everthing again
  for (int i = 0; i < size; i++) {
    key--;
    value--;
    ASSERT_TRUE(ht.Remove(nullptr, key, value));
    if ((i + 1) % 30000 == 0) {
      LOG_INFO("%d k-v pairs have been removed", (i + 1));
    }
    if (i >= 210000 && (i + 1) % max_elements_per_bucket == 0) {
      ht.VerifyIntegrity();
    }
  }
  LOG_INFO("%d k-v pairs have been removed", size);
  ht.VerifyIntegrityAndPrint(0, true);
  // the hash table should go back to its initial form
  ASSERT_EQ(0, ht.GetGlobalDepth());

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, MultiThreadSampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(128, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // based on the size of HashTableBucketPage<int, int, IntComparator>
  const int max_elements_per_bucket = 496;
  // a number of elements that makes the hash table just full
  const int max_elements_num = max_elements_per_bucket * DIRECTORY_ARRAY_SIZE;

  const int thread_num = 4;
  std::srand(std::time(nullptr));
  auto random_func = [&ht](int iteration_num, int mod) {
    std::ostringstream oss;
    oss << std::this_thread::get_id();
    for (int i = 0; i < iteration_num; i++) {
      int key = std::rand() % mod;
      int value = key;
      if (std::rand() % 2 == 0) {
        (void)ht.Insert(nullptr, key, value);
      } else {
        (void)ht.Remove(nullptr, key, value);
      }
      if ((i + 1) % 20000 == 0) {
        // NOLINTNEXTLINE
        LOG_INFO("Thread %s has already accessed the hash table %d times", oss.str().c_str(), (i + 1));
        ht.VerifyIntegrity();
      }
    }
  };

  std::thread workers[thread_num];
  for (auto &worker : workers) {
    worker = std::thread(random_func, 100000, max_elements_num);
  }

  for (auto &worker : workers) {
    worker.join();
  }
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, MultiThreadThrashingTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);

  const int iteration_num = 8;
  const int thread_num = 4;
  auto devil_func = [&bpm](int iteration_num, page_id_t page_id) {
    for (int i = 0; i < iteration_num; i++) {
      Page *page = bpm->FetchPage(page_id);
      if (page == nullptr) {
        continue;
      }
      // sleep for 20 ms
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      ASSERT_TRUE(bpm->UnpinPage(page_id, false));
      // sleep for another 5 ms
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  };

  std::thread prankers[thread_num];
  page_id_t page_id_temp = INVALID_PAGE_ID;
  for (int i = 0; i < thread_num; i++) {
    ASSERT_NE(nullptr, bpm->NewPage(&page_id_temp));
    ASSERT_EQ(i, page_id_temp);
    ASSERT_TRUE(bpm->UnpinPage(page_id_temp, false));
    prankers[i] = std::thread(devil_func, iteration_num, i);
  }

  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());
  for (int i = 0; i < iteration_num; i++) {
    if (i % 2 == 0) {
      (void)ht.Insert(nullptr, i, i);
    } else {
      (void)ht.Remove(nullptr, i, i);
    }
    // sleep for 10 ms
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  for (auto &pranker : prankers) {
    pranker.join();
  }
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
