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
}

// TODO: do we need to make sure ctor and dctor are also thread-safe?
LRUReplacer::~LRUReplacer() {
  // clear the lru map
  lru_map_.clear();

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

  if (lru_map_.size() == 0) {
    assert(dummy_->next == dummy_);
    assert(dummy_->prev == dummy_);
    LOG_ERROR("Trying to get a victim out of an empty lru.");
    return false;
  }
  frame_id_t fid = dummy_->prev->frame_id;
  assert(lru_map_.count(fid) > 0);

  // The following steps are basically the same with Pin() method, but we don't
  // want to reuse the method cuz Victim() is not equivalent to Pin(). Each
  // method would be better used in a single-responsibility way.
  LRUNode *node = lru_map_[fid];
  lru_map_.erase(fid);
  ListDelete(node);

  *frame_id = fid;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::unique_lock lock(mutex_);

  if (lru_map_.count(frame_id) == 0) {
    LOG_ERROR("Trying to pin an unknown frame, id = %d", frame_id);
    return;
  }
  LRUNode *node = lru_map_[frame_id];
  // remove it from the map first
  lru_map_.erase(frame_id);

  // then remove it from the list
  ListDelete(node);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::unique_lock lock(mutex_);

  if (lru_map_.count(frame_id) > 0) {
    LOG_WARN("Trying to unpin a frame (id = %d) multiple times, maybe dangerous in the upper level", frame_id);
    return;
  }
  if (lru_map_.size() == num_pages_) {
    LOG_ERROR("The LRU list is already full. WRONG CALLING!");
    return;
  }
  // insert into the list first
  LRUNode *node = new LRUNode(frame_id);
  ListInsert(node);

  // then insert into the map
  lru_map_.emplace(frame_id, node);
}

size_t LRUReplacer::Size() {
  std::shared_lock lock(mutex_);
  return lru_map_.size();
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

}  // namespace bustub
