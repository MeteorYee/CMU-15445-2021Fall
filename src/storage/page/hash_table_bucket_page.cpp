//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename Type>
static uint32_t GetHammingWeight(Type value) {
  Type num = value;
  static Type zero = static_cast<Type>(0);
  static Type one = static_cast<Type>(1);
  assert(num >= zero);

  // Brian Kernighan’s Algorithm
  uint32_t hamming_weight = 0;
  while (num > zero) {
    num &= (num - one);
    hamming_weight++;
  }
  return hamming_weight;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; idx++) {
    if (!IsOccupied(idx) || !IsReadable(idx)) {
      continue;
    }
    if (cmp(key, KeyAt(idx)) == 0) {
      result->push_back(ValueAt(idx));
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; idx++) {
    if (!IsOccupied(idx)) {
      SetOccupied(idx);
      SetReadable(idx);
      array_[idx] = std::make_pair(key, value);
      return true;
    }
    // must be occupied, but could be a tombstone
    if (!IsReadable(idx)) {
      // we just reuse the tombstone
      SetReadable(idx);
      array_[idx] = std::make_pair(key, value);
      return true;
    }
    if (cmp(key, KeyAt(idx)) == 0 && value == ValueAt(idx)) {
      // duplicate!
      return false;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; idx++) {
    if (!IsOccupied(idx) || !IsReadable(idx)) {
      continue;
    }
    if (!(cmp(key, KeyAt(idx)) == 0 && value == ValueAt(idx))) {
      continue;
    }
    RemoveAt(idx);
    return true;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  assert(bucket_idx < BUCKET_ARRAY_SIZE);
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  assert(bucket_idx < BUCKET_ARRAY_SIZE);
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  uint32_t bit_idx = bucket_idx / 8;
  char bit = static_cast<char>(0x1) << (7 - bucket_idx % 8);
  // notice that we don't clear the occupied bit here, hence make it a tombstone
  readable_[bit_idx] &= ~bit;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::InsertAt(uint32_t bucket_idx, KeyType key, ValueType value, KeyComparator cmp) {
  assert(!IsReadable(bucket_idx));
  SetOccupied(bucket_idx);
  SetReadable(bucket_idx);
  array_[bucket_idx] = std::make_pair(key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  assert(bucket_idx < BUCKET_ARRAY_SIZE);
  int shift_length = 7 - (bucket_idx % 8);
  const char *bitmap = occupied_ + (bucket_idx / 8);
  return (*bitmap & (static_cast<char>(0x1) << shift_length)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  assert(bucket_idx < BUCKET_ARRAY_SIZE);
  int shift_length = 7 - (bucket_idx % 8);
  char *bitmap = occupied_ + (bucket_idx / 8);
  *bitmap |= (static_cast<char>(0x1) << shift_length);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  assert(bucket_idx < BUCKET_ARRAY_SIZE);
  int shift_length = 7 - (bucket_idx % 8);
  const char *bitmap = readable_ + (bucket_idx / 8);
  return (*bitmap & (static_cast<char>(0x1) << shift_length)) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  assert(bucket_idx < BUCKET_ARRAY_SIZE);
  int shift_length = 7 - (bucket_idx % 8);
  char *bitmap = readable_ + (bucket_idx / 8);
  *bitmap |= (static_cast<char>(0x1) << shift_length);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  int tail_bits = BUCKET_ARRAY_SIZE % 8;
  char tail_byte = 0xff;
  if (tail_bits > 0) {
    int shift = 8 - tail_bits;
    tail_byte = (tail_byte >> shift) << shift;
  }
  // check the last byte first
  uint32_t length = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  if ((readable_[length - 1] ^ tail_byte) != 0) {
    return false;
  }
  // check the trailing bytes except the last one, because they are not enough
  // to construct a 64-bit word
  char check_byte = 0xff;
  uint32_t limit = ((length - 1) >> 3) << 3;
  for (uint32_t i = limit; i < length - 1; i++) {
    if ((readable_[i] ^ check_byte) != 0) {
      return false;
    }
  }
  // finally check the array with a 64-bit stride
  uint64_t check_word = 0xffffffffffffffff;
  uint64_t *word_ptr = reinterpret_cast<uint64_t *>(readable_);
  for (uint32_t i = 0; i < limit; i += 8, word_ptr++) {
    if ((*word_ptr ^ check_word) != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t count = 0;
  // for readable check, we don't need to handle the tail byte specifically as well
  // check the trailing bytes, because they are not enough to construct a 64-bit word
  uint32_t length = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  uint32_t limit = ((length - 1) >> 3) << 3;
  for (uint32_t i = limit; i < length; i++) {
    // notice the unsigned cast here, otherwise there will be undefined behaviors
    count += GetHammingWeight<unsigned char>(readable_[i]);
  }
  // finally check the array with a stride of 64-bit length
  uint64_t *word_ptr = reinterpret_cast<uint64_t *>(readable_);
  for (uint32_t i = 0; i < limit; i += 8, word_ptr++) {
    count += GetHammingWeight<uint64_t>(*word_ptr);
  }
  return count;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  // for empty check, we don't need to handle the last byte specifically, cuz the
  // redundant bits are guaranteed to be zeros
  // check the trailing bytes, because they are not enough to construct a 64-bit word
  uint32_t length = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  char check_byte = 0x00;
  uint32_t limit = ((length - 1) >> 3) << 3;
  for (uint32_t i = limit; i < length; i++) {
    if ((readable_[i] | check_byte) != 0) {
      return false;
    }
  }
  // finally check the array with a stride of 64-bit length
  uint64_t check_word = 0x00;
  uint64_t *word_ptr = reinterpret_cast<uint64_t *>(readable_);
  for (uint32_t i = 0; i < limit; i += 8, word_ptr++) {
    if ((*word_ptr | check_word) != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;
template class HashTableBucketPage<int64_t, int64_t, Int64Comparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
