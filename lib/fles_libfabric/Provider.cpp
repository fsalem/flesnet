// Copyright 2016 Thorsten Schuett <schuett@zib.de>, Farouk Salem <salem@zib.de>

#include "Provider.hpp"
#include "MsgGNIProvider.hpp"
#include "RDMGNIProvider.hpp"
#include "MsgSocketsProvider.hpp"
#include "RDMSocketsProvider.hpp"
#include "MsgVerbsProvider.hpp"
#include "RxMVerbsProvider.hpp"

#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>

#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>

#include "LibfabricException.hpp"

namespace tl_libfabric
{

std::unique_ptr<Provider> Provider::get_provider(std::string local_host_name)
{
    // std::cout << "Provider::get_provider()" << std::endl;
    struct fi_info* info = MsgVerbsProvider::exists(local_host_name);
    if (info != nullptr) {
        std::cout << "found Verbs" << std::endl;
        return std::unique_ptr<Provider>(new MsgVerbsProvider(info));
    }

    info = RxMVerbsProvider::exists(local_host_name);
    if (info != nullptr) {
	std::cout << "found RxM Verbs" << std::endl;
	return std::unique_ptr<Provider>(new RxMVerbsProvider(info));
    }

    info = MsgGNIProvider::exists(local_host_name);
	if (info != nullptr) {
		std::cout << "found MSG GNI" << std::endl;
		return std::unique_ptr<Provider>(new MsgGNIProvider(info));
	}

    info = RDMGNIProvider::exists(local_host_name);
    if (info != nullptr) {
        std::cout << "found RDM GNI" << std::endl;
        return std::unique_ptr<Provider>(new RDMGNIProvider(info));
    }

    info = MsgSocketsProvider::exists(local_host_name);
    if (info != nullptr) {
        std::cout << "found Sockets" << std::endl;
        return std::unique_ptr<Provider>(new MsgSocketsProvider(info));
    }

    info = RDMSocketsProvider::exists(local_host_name);
    if (info != nullptr) {
        std::cout << "found rdm" << std::endl;
        return std::unique_ptr<Provider>(new RDMSocketsProvider(info));
    }

    throw LibfabricException("no known Libfabric provider found");
}

uint64_t Provider::requested_key = 0;

std::unique_ptr<Provider> Provider::prov;

int Provider::vector = 0;
}
