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

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages): num_pages(num_pages) {
  m_lruMap.reserve(num_pages);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
//  std::lock_guard<std::mutex> guard(m_lock);
  std::scoped_lock<std::mutex> lock(m_lock);
  if (Size() == 0) {
    return false;
  }

  *frame_id = m_lruList.back();
  m_lruMap.erase(m_lruList.back());
  m_lruList.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
//  std::lock_guard<std::mutex> guard(m_lock);
  std::scoped_lock<std::mutex> lock(m_lock);

  if (m_lruMap.count(frame_id)) {
    m_lruList.erase(m_lruMap[frame_id]);
    m_lruMap.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
//  std::lock_guard<std::mutex> guard(m_lock);
  std::scoped_lock<std::mutex> lock(m_lock);

  // key already exists, just update the queue
  if(m_lruMap.find(frame_id) == m_lruMap.end()){
    m_lruList.push_front(frame_id);
    m_lruMap[frame_id] = m_lruList.begin();
  }
}

size_t LRUReplacer::Size() {
  return m_lruMap.size();
}

}  // namespace bustub
