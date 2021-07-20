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
#include <algorithm>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {

}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
	mut.lock();
	if (list.empty()) {
		mut.unlock();
		return false;
	}
	*frame_id = list.back();
	list.pop_back();
	mut.unlock();
	return true;

}

void LRUReplacer::Pin(frame_id_t frame_id) {
	mut.lock();
	auto iterator = std::find(list.begin(), list.end(), frame_id);
	if (iterator != list.end())
	{
		list.erase(iterator);
	}
	mut.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
	mut.lock();
	auto iterator = std::find(list.begin(), list.end(), frame_id);
	if (iterator != list.end())
	{
		// list.erase(iterator);
		mut.unlock();
		return;
	}
	list.insert(list.begin(), frame_id);
	mut.unlock();
}

size_t LRUReplacer::Size() {
	mut.lock();
	int size = list.size();
	mut.unlock();
	return size;
}

}  // namespace bustub
