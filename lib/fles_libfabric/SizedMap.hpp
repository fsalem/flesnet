// Copyright 2017 Farouk Salem <salem@zib.de>
#pragma once

#include <map>

namespace tl_libfabric {
template <typename KEY, typename VALUE>
class SizedMap
{
public:
	SizedMap(uint32_t max_map_size);
	SizedMap() = delete;
	SizedMap(const SizedMap&) = delete;
	SizedMap& operator=(const SizedMap&) = delete;

	bool add(const KEY key, const VALUE val);

	bool update(const KEY key, const VALUE val);

	bool remove(const KEY key);

	bool contains(const KEY key) const;

	uint32_t size() const;

	VALUE get(const KEY key) const;

	KEY get_last_key() const;

private:

	typename std::map<KEY,VALUE> map_;
    const uint32_t MAX_MAP_SIZE_;
};


template <typename KEY, typename VALUE>
SizedMap<KEY,VALUE>::SizedMap(uint32_t max_map_size) :
	MAX_MAP_SIZE_(max_map_size){

}

template <typename KEY, typename VALUE>
bool SizedMap<KEY,VALUE>::add(const KEY key, const VALUE val)
{
	if (map_.find(key) != map_.end()) {
		return false;
	}

	if (map_.size() == MAX_MAP_SIZE_) {
		map_.erase(map_.begin());
	}

	map_.insert (std::pair<KEY,VALUE>(key,val));

	return true;

}

template <typename KEY, typename VALUE>
bool SizedMap<KEY,VALUE>::update(const KEY key, const VALUE val)
{
	typename std::map<KEY,VALUE>::iterator it = map_.find(key);
	if (it == map_.end()) {
		return false;
	}

	it->second = val;

	return true;

}

template <typename KEY, typename VALUE>
bool SizedMap<KEY,VALUE>::remove(const KEY key)
{
	typename std::map<KEY,VALUE>::iterator it = map_.find(key);
	if (it == map_.end()) {
		return false;
	}

	map_.erase(it);

	return true;
}

template <typename KEY, typename VALUE>
bool SizedMap<KEY,VALUE>::contains(const KEY key) const {
	return false ? map_.find(key) == map_.end() : true;
}

template <typename KEY, typename VALUE>
uint32_t SizedMap<KEY,VALUE>::size() const {

	return map_.size();
}

template <typename KEY, typename VALUE>
VALUE SizedMap<KEY,VALUE>::get(const KEY key) const {

	return map_.find(key)->second;
}

template <typename KEY, typename VALUE>
KEY SizedMap<KEY,VALUE>::get_last_key() const {

	return (--map_.end())->first;
}

} //namespace tl_libfabric
