//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUNode::LRUNode() { frame_id = -1; }

LRUReplacer::LRUNode::LRUNode(frame_id_t frm_id) : frame_id{frm_id} {}

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_{num_pages} {
  dummy_ = new LRUNode(-1);
  dummy_->next = dummy_;
  dummy_->prev = dummy_;

  frame_count_ = 0;
  lru_vec_.insert(lru_vec_.end(), num_pages, nullptr);
}

// TODO: do we need to make sure ctor and dctor are also thread-safe?
LRUReplacer::~LRUReplacer() {
  // clear the lru vector
  lru_vec_.clear();

  // release the lru list
  while (dummy_->next != dummy_) {
    ListDelete(dummy_->next);
  }

  // release the dummy head
  dummy_->next = nullptr;
  dummy_->prev = nullptr;
  delete dummy_;
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::unique_lock lock(mutex_);

  if (frame_count_ == 0) {
    assert(dummy_->next == dummy_);
    assert(dummy_->prev == dummy_);
    LOG_ERROR("Trying to get a victim out of an empty lru.");
    return false;
  }
  frame_id_t fid = dummy_->prev->frame_id;
  assert(lru_vec_[fid] != nullptr);

  // The following steps are basically the same with Pin() method, but we don't
  // want to reuse the method cuz Victim() is not equivalent to Pin(). Each
  // method would be better used in a single-responsibility way.
  LRUNode *node = lru_vec_[fid];
  lru_vec_[fid] = nullptr;
  ListDelete(node);
  frame_count_--;

  *frame_id = fid;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (!IsIdValid(static_cast<size_t>(frame_id))) {
    LOG_ERROR("Invalid frame id = %d!", frame_id);
    return;
  }

  std::unique_lock lock(mutex_);

  if (lru_vec_[frame_id] == nullptr) {
    LOG_ERROR("Trying to pin a non-existing frame, id = %d", frame_id);
    return;
  }
  LRUNode *node = lru_vec_[frame_id];
  // remove it from the vector first
  lru_vec_[frame_id] = nullptr;

  // then remove it from the list
  ListDelete(node);
  frame_count_--;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (!IsIdValid(static_cast<size_t>(frame_id))) {
    LOG_ERROR("Invalid frame id = %d!", frame_id);
    return;
  }

  std::unique_lock lock(mutex_);

  if (lru_vec_[frame_id] != nullptr) {
    LOG_WARN("Trying to unpin a frame (id = %d) multiple times, maybe dangerous in the upper level", frame_id);
    return;
  }
  // insert into the list first
  LRUNode *node = new LRUNode(frame_id);
  ListInsert(node);

  // then insert into the vector
  lru_vec_[frame_id] = node;
  frame_count_++;
}

size_t LRUReplacer::Size() {
  std::shared_lock lock(mutex_);
  return frame_count_;
}

void LRUReplacer::ListDelete(LRUNode *node) {
  node->next->prev = node->prev;
  node->prev->next = node->next;
  node->next = nullptr;
  node->prev = nullptr;
  delete node;
}

void LRUReplacer::ListInsert(LRUNode *node) {
  dummy_->next->prev = node;
  node->next = dummy_->next;
  dummy_->next = node;
  node->prev = dummy_;
}

bool LRUReplacer::IsIdValid(frame_id_t fid) { return fid >= 0 && static_cast<size_t>(fid) < num_pages_; }

}  // namespace bustub
