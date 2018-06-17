// Copyright 2018 Farouk Salem <salem@zib.de>
#pragma once

#include <set>
#include <assert.h>
#include "ConstVariables.hpp"

namespace tl_libfabric {
template <typename KEY>
class SizedSet
{
public:
    SizedSet(uint32_t max_map_size);
    SizedSet();

    void add(const KEY key);

    uint32_t size() const;

    uint32_t count(const KEY key) const;

    KEY get_last_key() const;

    KEY get_median_key() const;

    typename std::multiset<KEY>::iterator get_begin_iterator();

    typename std::multiset<KEY>::iterator get_iterator(const KEY key);

    typename std::multiset<KEY>::iterator get_end_iterator();

    typedef typename std::multiset<KEY>::iterator iterator;

private:

    typename std::multiset<KEY> set_;
    const uint32_t MAX_SET_SIZE_;
};


template <typename KEY>
SizedSet<KEY>::SizedSet(uint32_t max_set_size) :
    MAX_SET_SIZE_(max_set_size){

}

template <typename KEY>
SizedSet<KEY>::SizedSet() :
    MAX_SET_SIZE_(ConstVariables::MAX_HISTORY_SIZE){
}

template <typename KEY>
void SizedSet<KEY>::add(const KEY key)
{
    // TODO only add up to MAX_SET_SIZE_ elements!!
    set_.insert(key);
}

template <typename KEY>
uint32_t SizedSet<KEY>::size() const {
    return set_.size();
}

template <typename KEY>
uint32_t SizedSet<KEY>::count(const KEY key) const {
    return set_.count(key);
}

template <typename KEY>
KEY SizedSet<KEY>::get_last_key() const {
    return (*(--set_.end()));
}

template <typename KEY>
KEY SizedSet<KEY>::get_median_key() const {
    if (set_.empty())return 0;

    auto it = set_.cbegin();
    std::advance(it, size() / 2);

    if (size() % 2 == 0){
	return (*it+*(--it))/2;
    }
    return (*it);
}

template <typename KEY>
typename std::multiset<KEY>::iterator SizedSet<KEY>::get_begin_iterator() {
    return set_.cbegin();
}

template <typename KEY>
typename std::multiset<KEY>::iterator SizedSet<KEY>::get_iterator(const KEY key) {
    return set_.find(key);
}

template <typename KEY>
typename std::multiset<KEY>::iterator SizedSet<KEY>::get_end_iterator() {
    return set_.cend();
}
} //namespace tl_libfabric
