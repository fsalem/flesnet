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
    struct fi_custom_context* context;
    if (available_.empty()){
	context = new fi_custom_context();
	context->id = context_counter_++;
	L_(debug) << "getContext:: New context is created with ID " << context->id;
    }else{
	context = &available_[0];
	available_.erase(available_.begin());
    }
    pool_mutex_.unlock();
    in_use_.push_back(*context);
    L_(debug) << "getContext:: context is in using with ID " << context->id << " --> available = " << available_.size() << ", in use = " << in_use_.size();
    log();
    return context;
}

void LibfabricContextPool::releaseContext(struct fi_custom_context* context) {
    uint32_t count = 0;
    while (count < in_use_.size()){
	if (context->id == in_use_[count].id){
	    available_.push_back(in_use_[count]);
	    in_use_.erase(in_use_.begin()+count);
	    break;
	}
	++count;
    }
    L_(debug) << "LibfabricContextPool: Context released with ID " << context->id << " --> available = " << available_.size() << ", in use = " << in_use_.size();
    log();
}

void LibfabricContextPool::log() {
    for (int i=0 ; i < available_.size() ; i++){
	L_(debug) << "Logging:: Available[" << i << "].id = " << available_[i].id;
    }
    for (int i=0 ; i < in_use_.size() ; i++){
	L_(debug) << "Logging:: in_use_[" << i << "].id = " << in_use_[i].id;
    }
}

std::unique_ptr<LibfabricContextPool> LibfabricContextPool::context_pool_ = nullptr;

}
