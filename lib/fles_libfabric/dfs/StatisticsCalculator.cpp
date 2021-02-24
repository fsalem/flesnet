// Copyright 2020 Farouk Salem <salem@zib.de>

#include "StatisticsCalculator.hpp"
#include "log.hpp"

namespace tl_libfabric {

uint64_t StatisticsCalculator::get_median_of_list(std::vector<uint64_t> list) {
  std::sort(list.begin(), list.end());
  return list[(list.size() / 2)];
}

StatisticsCalculator::ListStatistics
StatisticsCalculator::calculate_list_statistics(std::vector<uint64_t> list) {
  // TODO assert(!list.empty());
  StatisticsCalculator::ListStatistics stats;
  if (list.empty()) {
    L_(fatal) << "[calculate_list_statistics] EMPTY LIST!!!";
    return stats;
  }
  stats.min_val = list[0], stats.max_val = list[0], stats.sum_val = list[0];
  stats.min_indx = 0, stats.max_indx = 0;
  for (uint32_t indx = 1; indx < list.size(); ++indx) {
    if (list[indx] < stats.min_val) {
      stats.min_val = list[indx];
      stats.min_indx = indx;
    }
    if (list[indx] > stats.max_val) {
      stats.max_val = list[indx];
      stats.max_indx = indx;
    }
    stats.sum_val += list[indx];
  }
  stats.median_val = get_median_of_list(list);
  return stats;
}

} /* namespace tl_libfabric */
