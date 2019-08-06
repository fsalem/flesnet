// Copyright 2019 Farouk Salem <salem@zib.de>

#include "LibfabricContextPool.hpp"

namespace tl_libfabric
{


std::unique_ptr<LibfabricContextPool>& LibfabricContextPool::getInst() {
    if (LibfabricContextPool::context_pool_ == nullptr)
	LibfabricContextPool::context_pool_ = std::unique_ptr<LibfabricContextPool>(new LibfabricContextPool());

    return LibfabricContextPool::context_pool_;
}

LibfabricContextPool::LibfabricContextPool() {
    ///
}
LibfabricContextPool::~LibfabricContextPool(){
    available_.clear();
    in_use_.clear();
    LibfabricContextPool::context_pool_ = nullptr;
    L_(info) << "LibfabricContextPool deconstructor: Total number of created objects " << context_counter_;
}

struct fi_custom_context* LibfabricContextPool::getContext() {
    pool_mutex_.lock();
    struct fi_custom_context context;
    if (available_.empty()){
	context.id = context_counter_++;
    }else{
	context = available_[0];
	available_.erase(available_.begin());
    }
    pool_mutex_.unlock();
    in_use_.push_back(context);
    return &context;
}

void LibfabricContextPool::releaseContext(struct fi_custom_context* context) {
    L_(debug) << "LibfabricContextPool: Context released with ID " << context->id;
    available_.push_back(*context);
}

std::unique_ptr<LibfabricContextPool> LibfabricContextPool::context_pool_ = nullptr;

}
