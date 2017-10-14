// Copyright 2012-2016 Jan de Cuveland <cmail@cuveland.de>

#include "Application.hpp"
#include "ChildProcessManager.hpp"
#include "EmbeddedPatternGenerator.hpp"
#include "FlibPatternGenerator.hpp"
#include "shm_channel_client.hpp"
#include <boost/algorithm/string.hpp>
#include <boost/thread/future.hpp>
#include <boost/thread/thread.hpp>
#include <log.hpp>
#include <random>
#include <string>

Application::Application(Parameters const& par,
                         volatile sig_atomic_t* signal_status)
    : par_(par), signal_status_(signal_status)
{
    unsigned input_nodes_size = par.input_nodes().size();
    std::vector<unsigned> input_indexes = par.input_indexes();

    // FIXME: some of this is a terrible mess
    if (!par.input_shm().empty()) {
        try {
            shm_device_ =
                std::make_shared<flib_shm_device_client>(par.input_shm());
            shm_num_channels_ = shm_device_->num_channels();
            L_(info) << "using shared memory";

            // increase number of input nodes to match number of
            // enabled FLIB links if in stand-alone mode
            if (par.standalone() && shm_num_channels_ > 1) {
                input_nodes_size = shm_num_channels_;
                for (unsigned i = 1; i < input_nodes_size; i++) {
                    input_indexes.push_back(i);
                }
            }

        } catch (std::exception const& e) {
            L_(error) << "exception while connecting to shared memory: "
                      << e.what();
        }
    }
    // end FIXME

    if (par.standalone()) {
        L_(info) << "flesnet in stand-alone mode, inputs: " << input_nodes_size;
    }

    // Compute node application

    // set_cpu(1);

    std::vector<std::string> input_server_addresses;
    for (unsigned int i = 0; i < par.input_nodes().size(); ++i)
        input_server_addresses.push_back("tcp://" + par.input_nodes().at(i) +
                                         ":" +
                                         std::to_string(par.base_port() + i));

    for (unsigned i : par_.compute_indexes()) {
        // generate random shared memory identifier for timeslice buffer
        std::random_device random_device;
        std::uniform_int_distribution<uint64_t> uint_distribution;
        uint64_t random_number = uint_distribution(random_device);
        std::string shm_identifier = "flesnet_" + std::to_string(random_number);

        std::unique_ptr<TimesliceBuffer> tsb(new TimesliceBuffer(
            shm_identifier, par_.cn_data_buffer_size_exp(),
            par_.cn_desc_buffer_size_exp(), input_nodes_size));

        start_processes(shm_identifier);
        ChildProcessManager::get().allow_stop_processes(this);

        if (par_.zeromq()) {
            std::unique_ptr<TimesliceBuilderZeromq> builder(
                new TimesliceBuilderZeromq(i, *tsb, input_server_addresses,
                                           par_.timeslice_size()));
            timeslice_builders_zeromq_.push_back(std::move(builder));
        } else if (par_.use_libfabric()) {
#ifdef LIBFABRIC
            std::unique_ptr<tl_libfabric::TimesliceBuilder> builder(
                new tl_libfabric::TimesliceBuilder(
                    i, *tsb, par_.base_port() + i, input_nodes_size,
                    par_.timeslice_size(), signal_status_, false,
                    par_.compute_nodes()[i],
                    par_.min_recv_ts_to_process()));
            timeslice_builders_.push_back(std::move(builder));
#else
            L_(fatal) << "flesnet built without LIBFABRIC support";
#endif
        } else {
#ifdef RDMA
            std::unique_ptr<TimesliceBuilder> builder(new TimesliceBuilder(
                i, *tsb, par_.base_port() + i, input_nodes_size,
                par_.timeslice_size(), signal_status_, false));
            timeslice_builders_.push_back(std::move(builder));
#else
            L_(fatal) << "flesnet built without RDMA support";
#endif
        }

        timeslice_buffers_.push_back(std::move(tsb));
    }

    set_node();

    // Input node application

    std::vector<std::string> compute_services;
    for (unsigned int i = 0; i < par.compute_nodes().size(); ++i)
        compute_services.push_back(std::to_string(par.base_port() + i));

    for (size_t c = 0; c < input_indexes.size(); ++c) {
        unsigned index = input_indexes.at(c);

        if (c < shm_num_channels_) {
            data_sources_.push_back(std::unique_ptr<InputBufferReadInterface>(
                new flib_shm_channel_client(shm_device_, c)));
        } else {
            if (false) {
                data_sources_.push_back(
                    std::unique_ptr<InputBufferReadInterface>(
                        new FlibPatternGenerator(par.in_data_buffer_size_exp(),
                                                 par.in_desc_buffer_size_exp(),
                                                 index,
                                                 par.typical_content_size(),
                                                 par.generate_ts_patterns(),
                                                 par.random_ts_sizes())));
            } else {
                data_sources_.push_back(
                    std::unique_ptr<InputBufferReadInterface>(
                        new EmbeddedPatternGenerator(
                            par.in_data_buffer_size_exp(),
                            par.in_desc_buffer_size_exp(), index,
                            par.typical_content_size(),
                            par.generate_ts_patterns(),
                            par.random_ts_sizes())));
            }
        }

        if (par_.zeromq()) {
            std::string listen_address =
                "tcp://*:" + std::to_string(par_.base_port() + index);
            std::unique_ptr<ComponentSenderZeromq> sender(
                new ComponentSenderZeromq(*(data_sources_.at(c).get()),
                                          par.timeslice_size(),
                                          par.overlap_size(), listen_address));
            component_senders_zeromq_.push_back(std::move(sender));
        } else if (par_.use_libfabric()) {
#ifdef LIBFABRIC
            std::unique_ptr<tl_libfabric::InputChannelSender> sender(
                new tl_libfabric::InputChannelSender(
                    index, *(data_sources_.at(c).get()), par.compute_nodes(),
                    compute_services, par.timeslice_size(), par.overlap_size(),
                    par.max_timeslice_number(), par.input_nodes().at(index), par.init_wait_time()));
            input_channel_senders_.push_back(std::move(sender));
#else
            L_(fatal) << "flesnet built without LIBFABRIC support";
#endif
        } else {
#ifdef RDMA
            std::unique_ptr<InputChannelSender> sender(new InputChannelSender(
                index, *(data_sources_.at(c).get()), par.compute_nodes(),
                compute_services, par.timeslice_size(), par.overlap_size(),
                par.max_timeslice_number()));
            input_channel_senders_.push_back(std::move(sender));
#else
            L_(fatal) << "flesnet built without RDMA support";
#endif
        }
    }
}

Application::~Application() {}

void Application::run()
{
// Do not spawn additional thread if only one is needed, simplifies
// debugging
#if defined(RDMA) || defined(LIBFABRIC)
    if (timeslice_builders_.size() == 1 && input_channel_senders_.empty()) {
        L_(debug) << "using existing thread for single timeslice builder";
        (*timeslice_builders_[0])();
        return;
    };
    if (input_channel_senders_.size() == 1 && timeslice_builders_.empty()) {
        L_(debug) << "using existing thread for single input channel sender";
        (*input_channel_senders_[0])();
        return;
    };
#endif

    // FIXME: temporary code, need to implement interrupt
    boost::thread_group threads;
    std::vector<boost::unique_future<void>> futures;
    bool stop = false;

#if defined(RDMA) || defined(LIBFABRIC)
    for (auto& buffer : timeslice_builders_) {
        boost::packaged_task<void> task(std::ref(*buffer));
        futures.push_back(task.get_future());
        threads.add_thread(new boost::thread(std::move(task)));
    }

    for (auto& buffer : input_channel_senders_) {
        boost::packaged_task<void> task(std::ref(*buffer));
        futures.push_back(task.get_future());
        threads.add_thread(new boost::thread(std::move(task)));
    }
#endif

    for (auto& buffer : timeslice_builders_zeromq_) {
        boost::packaged_task<void> task(std::ref(*buffer));
        futures.push_back(task.get_future());
        threads.add_thread(new boost::thread(std::move(task)));
    }

    for (auto& buffer : component_senders_zeromq_) {
        boost::packaged_task<void> task(std::ref(*buffer));
        futures.push_back(task.get_future());
        threads.add_thread(new boost::thread(std::move(task)));
    }

    L_(debug) << "threads started: " << threads.size();

    while (!futures.empty()) {
        auto it = boost::wait_for_any(futures.begin(), futures.end());
        try {
            it->get();
        } catch (const std::exception& e) {
            L_(fatal) << "exception from thread: " << e.what();
            stop = true;
        }
        futures.erase(it);
        if (stop)
            threads.interrupt_all();
    }

    threads.join_all();
}

void Application::start_processes(const std::string shared_memory_identifier)
{
    const std::string processor_executable = par_.processor_executable();
    assert(!processor_executable.empty());
    for (uint_fast32_t i = 0; i < par_.processor_instances(); ++i) {
        std::stringstream index;
        index << i;
        ChildProcess cp = ChildProcess();
        cp.owner = this;
        boost::split(cp.arg, processor_executable, boost::is_any_of(" \t"),
                     boost::token_compress_on);
        cp.path = cp.arg.at(0);
        for (auto& arg : cp.arg) {
            boost::replace_all(arg, "%s", shared_memory_identifier);
            boost::replace_all(arg, "%i", index.str());
        }
        ChildProcessManager::get().start_process(cp);
    }
}
