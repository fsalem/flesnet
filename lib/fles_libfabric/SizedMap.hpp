// Copyright 2017 Farouk Salem <salem@zib.de>
#pragma once

#include <map>
#include <set>
#include <assert.h>
#include "ConstVariables.hpp"

namespace tl_libfabric {
template <typename KEY, typename VALUE>
class SizedMap
{
public:
    SizedMap(uint32_t max_map_size);
    SizedMap();
    //SizedMap(const SizedMap&) = delete;
    //SizedMap& operator=(const SizedMap&) = delete;

    bool add(const KEY key, const VALUE val);

    bool update(const KEY key, const VALUE val);

    bool remove(const KEY key);

    bool contains(const KEY key) const;

    uint32_t size() const;

    VALUE get(const KEY key) const;

    KEY get_last_key() const;

    VALUE get_median_value() const;

    typename std::map<KEY,VALUE>::iterator get_begin_iterator();

    typename std::map<KEY,VALUE>::iterator get_iterator(const KEY key);

    typename std::map<KEY,VALUE>::iterator get_end_iterator();

    typedef typename std::map<KEY,VALUE>::iterator iterator;

private:

    typename std::map<KEY,VALUE> map_;
    // to be used for the median value
    typename std::multiset<VALUE> set_;
    const uint32_t MAX_MAP_SIZE_;
};


template <typename KEY, typename VALUE>
SizedMap<KEY,VALUE>::SizedMap(uint32_t max_map_size) :
    MAX_MAP_SIZE_(max_map_size){

}

template <typename KEY, typename VALUE>
SizedMap<KEY,VALUE>::SizedMap() :
    MAX_MAP_SIZE_(ConstVariables::MAX_HISTORY_SIZE){
}

template <typename KEY, typename VALUE>
bool SizedMap<KEY,VALUE>::add(const KEY key, const VALUE val)
{
    if (map_.find(key) != map_.end()) {
	return false;
    }

    if (map_.size() == MAX_MAP_SIZE_) {
	typename std::map<KEY,VALUE>::iterator begin = map_.begin();
	set_.erase(begin->second);
	map_.erase(begin);
	assert (map_.size() == MAX_MAP_SIZE_ - 1);
    }

    map_.insert (std::pair<KEY,VALUE>(key,val));
    set_.insert (val);

    return true;

}

template <typename KEY, typename VALUE>
bool SizedMap<KEY,VALUE>::update(const KEY key, const VALUE val)
{
    typename std::map<KEY,VALUE>::iterator it = map_.find(key);
    if (it == map_.end()) {
	return false;
    }
    set_.erase(it->second);
    it->second = val;
    set_.insert (it->second);

    return true;
}

template <typename KEY, typename VALUE>
bool SizedMap<KEY,VALUE>::remove(const KEY key)
{
    typename std::map<KEY,VALUE>::iterator it = map_.find(key);
    if (it == map_.end()) {
	return false;
    }


    set_.erase(it->second);
    map_.erase(it);

    return true;
}

template <typename KEY, typename VALUE>
bool SizedMap<KEY,VALUE>::contains(const KEY key) const {
    return map_.find(key) == map_.end() ? false : true;
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

template <typename KEY, typename VALUE>
VALUE SizedMap<KEY,VALUE>::get_median_value() const {
    if (set_.empty())return 0;

    auto it = set_.cbegin();
    std::advance(it, size() / 2);

    if (size() % 2 == 0){
	return (*it+*(--it))/2;
    }
    return (*it);
}

template <typename KEY, typename VALUE>
typename std::map<KEY,VALUE>::iterator SizedMap<KEY,VALUE>::get_begin_iterator() {
    return map_.begin();
}

template <typename KEY, typename VALUE>
typename std::map<KEY,VALUE>::iterator SizedMap<KEY,VALUE>::get_iterator(const KEY key) {
    return map_.find(key);
}

template <typename KEY, typename VALUE>
typename std::map<KEY,VALUE>::iterator SizedMap<KEY,VALUE>::get_end_iterator() {
    return map_.end();
}
} //namespace tl_libfabric
