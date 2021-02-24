// Copyright 2020 Farouk Salem <salem@zib.de>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <vector>

namespace tl_libfabric {

class StatisticsCalculator {
private:
  StatisticsCalculator() {}

public:
  struct ListStatistics {
    uint64_t min_val, max_val, median_val, sum_val;
    uint32_t min_indx, max_indx;
  };
  static uint64_t get_median_of_list(std::vector<uint64_t> list);
  static ListStatistics calculate_list_statistics(std::vector<uint64_t> list);
};

} /* namespace tl_libfabric */
