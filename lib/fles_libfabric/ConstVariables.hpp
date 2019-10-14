// Copyright 2017 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>


#pragma once

#include <iostream>

#pragma pack(1)
namespace tl_libfabric {
struct ConstVariables {
    const static uint64_t MINUS_ONE = -1;
    const static uint64_t ZERO = 0;
    const static uint32_t ONE_HUNDRED = 100;

    const static uint32_t MAX_HISTORY_SIZE = 20000; // high for logging ... should be small ~100-200

    const static uint16_t MAX_DESCRIPTOR_ARRAY_SIZE = 10;

    ///-----
    const static uint32_t MAX_MEDIAN_VALUES = 10;
    ///-----/
};
}
#pragma pack()
