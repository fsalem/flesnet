// Copyright 2012-2013 Jan de Cuveland <cmail@cuveland.de>

#include "InputChannelSender.hpp"
#include "MicrosliceDescriptor.hpp"
#include "RequestIdentifier.hpp"
#include "Utility.hpp"
#include <log.hpp>
#include <cassert>

InputChannelSender::InputChannelSender(
    uint64_t input_index, DataSource& data_source,
    const std::vector<std::string> compute_hostnames,
    const std::vector<std::string> compute_services, uint32_t timeslice_size,
    uint32_t overlap_size, uint32_t max_timeslice_number)
    : input_index_(input_index), data_source_(data_source),
      compute_hostnames_(compute_hostnames),
      compute_services_(compute_services), timeslice_size_(timeslice_size),
      overlap_size_(overlap_size), max_timeslice_number_(max_timeslice_number),
      min_acked_desc_(data_source.desc_buffer().size() / 4),
      min_acked_data_(data_source.data_buffer().size() / 4)
{
    size_t min_ack_buffer_size =
        data_source_.desc_buffer().size() / timeslice_size_ + 1;
    ack_.alloc_with_size(min_ack_buffer_size);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
    VALGRIND_MAKE_MEM_DEFINED(data_source_.data_buffer().ptr(),
                              data_source_.data_buffer().bytes());
    VALGRIND_MAKE_MEM_DEFINED(data_source_.desc_buffer().ptr(),
                              data_source_.desc_buffer().bytes());
#pragma GCC diagnostic pop
}

InputChannelSender::~InputChannelSender()
{
    if (mr_desc_) {
        ibv_dereg_mr(mr_desc_);
        mr_desc_ = nullptr;
    }

    if (mr_data_) {
        ibv_dereg_mr(mr_data_);
        mr_data_ = nullptr;
    }
}

void InputChannelSender::report_status()
{
    constexpr auto interval = std::chrono::seconds(1);

    // if data_source.written pointers are lagging behind due to lazy updates,
    // use sent value instead
    uint64_t written_desc = data_source_.written_desc();
    if (written_desc < sent_desc_) {
        written_desc = sent_desc_;
    }
    uint64_t written_data = data_source_.written_data();
    if (written_data < sent_data_) {
        written_data = sent_data_;
    }

    std::chrono::system_clock::time_point now =
        std::chrono::system_clock::now();
    SendBufferStatus status_desc{now, data_source_.desc_buffer().size(),
                                 cached_acked_desc_, acked_desc_, sent_desc_,
                                 written_desc};
    SendBufferStatus status_data{now, data_source_.data_buffer().size(),
                                 cached_acked_data_, acked_data_, sent_data_,
                                 written_data};

    double delta_t =
        std::chrono::duration<double, std::chrono::seconds::period>(
            status_desc.time - previous_send_buffer_status_desc_.time)
            .count();
    double rate_desc =
        static_cast<double>(status_desc.acked -
                            previous_send_buffer_status_desc_.acked) /
        delta_t;
    double rate_data =
        static_cast<double>(status_data.acked -
                            previous_send_buffer_status_data_.acked) /
        delta_t;

    L_(debug) << "[i" << input_index_ << "] desc " << status_desc.percentages()
              << " (used..free) | "
              << human_readable_count(status_desc.acked, true, "") << " ("
              << human_readable_count(rate_desc, true, "Hz") << ")";

    L_(debug) << "[i" << input_index_ << "] data " << status_data.percentages()
              << " (used..free) | "
              << human_readable_count(status_data.acked, true) << " ("
              << human_readable_count(rate_data, true, "B/s") << ")";

    L_(info) << "[i" << input_index_ << "]   |"
             << bar_graph(status_data.vector(), "#x_.", 20) << "|"
             << bar_graph(status_desc.vector(), "#x_.", 10) << "| "
             << human_readable_count(rate_data, true, "B/s");

    previous_send_buffer_status_desc_ = status_desc;
    previous_send_buffer_status_data_ = status_data;

    scheduler_.add(std::bind(&InputChannelSender::report_status, this),
                   now + interval);
}

void InputChannelSender::sync_buffer_positions()
{
    for (auto& c : conn_) {
        c->try_sync_buffer_positions();
    }

    auto now = std::chrono::system_clock::now();
    scheduler_.add(std::bind(&InputChannelSender::sync_buffer_positions, this),
                   now + std::chrono::milliseconds(0));
}

/// The thread main function.
void InputChannelSender::operator()()
{
    try {
        set_cpu(2);

        connect();
        while (connected_ != compute_hostnames_.size()) {
            poll_cm_events();
        }

        data_source_.proceed();
        time_begin_ = std::chrono::high_resolution_clock::now();

        uint64_t timeslice = 0;
        sync_buffer_positions();
        report_status();
        while (timeslice < max_timeslice_number_ && !abort_) {
            if (try_send_timeslice(timeslice)) {
                timeslice++;
            }
            poll_completion();
            data_source_.proceed();
            scheduler_.timer();
        }

        for (auto& c : conn_) {
            c->finalize(abort_);
        }

        L_(debug) << "[i" << input_index_ << "] "
                  << "SENDER loop done";

        while (!all_done_) {
            poll_completion();
            scheduler_.timer();
        }

        time_end_ = std::chrono::high_resolution_clock::now();

        disconnect();
        while (connected_ != 0) {
            poll_cm_events();
        }

        summary();
    } catch (std::exception& e) {
        L_(error) << "exception in InputChannelSender: " << e.what();
    }
}

bool InputChannelSender::try_send_timeslice(uint64_t timeslice)
{
    // wait until a complete TS is available in the input buffer
    uint64_t mc_offset = timeslice * timeslice_size_;
    uint64_t mc_length = timeslice_size_ + overlap_size_;

    // check if last microslice has really been written to memory
    if (data_source_.desc_buffer().at(mc_offset + mc_length).idx >
        previous_desc_idx_) {

        uint64_t data_offset = data_source_.desc_buffer().at(mc_offset).offset;
        uint64_t data_end =
            data_source_.desc_buffer().at(mc_offset + mc_length).offset;
        assert(data_end >= data_offset);

        uint64_t data_length = data_end - data_offset;
        uint64_t total_length =
            data_length + mc_length * sizeof(fles::MicrosliceDescriptor);

        if (false) {
            L_(trace) << "SENDER working on TS " << timeslice << ", MCs "
                      << mc_offset << ".." << (mc_offset + mc_length - 1)
                      << ", data bytes " << data_offset << ".."
                      << (data_offset + data_length - 1);
            L_(trace) << get_state_string();
        }

        int cn = target_cn_index(timeslice);

        if (!conn_[cn]->write_request_available())
            return false;

        // number of bytes to skip in advance (to avoid buffer wrap)
        uint64_t skip = conn_[cn]->skip_required(total_length);
        total_length += skip;

        if (conn_[cn]->check_for_buffer_space(total_length, 1)) {

            previous_desc_idx_ =
                data_source_.desc_buffer().at(mc_offset + mc_length).idx;

            post_send_data(timeslice, cn, mc_offset, mc_length, data_offset,
                           data_length, skip);

            conn_[cn]->inc_write_pointers(total_length, 1);

            sent_desc_ = mc_offset + mc_length;
            sent_data_ = data_end;

            return true;
        }
    }

    return false;
}

std::unique_ptr<InputChannelConnection>
InputChannelSender::create_input_node_connection(uint_fast16_t index)
{
    unsigned int max_send_wr = 8000;

    // limit pending write requests so that send queue and completion queue
    // do not overflow
    unsigned int max_pending_write_requests = std::min(
        static_cast<unsigned int>((max_send_wr - 1) / 3),
        static_cast<unsigned int>((num_cqe_ - 1) / compute_hostnames_.size()));

    std::unique_ptr<InputChannelConnection> connection(
        new InputChannelConnection(ec_, index, input_index_, max_send_wr,
                                   max_pending_write_requests));
    return connection;
}

void InputChannelSender::connect()
{
    for (unsigned int i = 0; i < compute_hostnames_.size(); ++i) {
        std::unique_ptr<InputChannelConnection> connection =
            create_input_node_connection(i);
        connection->connect(compute_hostnames_[i], compute_services_[i]);
        conn_.push_back(std::move(connection));
    }
}

int InputChannelSender::target_cn_index(uint64_t timeslice)
{
    return timeslice % conn_.size();
}

void InputChannelSender::dump_mr(struct ibv_mr* mr)
{
    L_(debug) << "[i" << input_index_ << "] "
              << "ibv_mr dump:";
    L_(debug) << " addr=" << reinterpret_cast<uint64_t>(mr->addr);
    L_(debug) << " length=" << static_cast<uint64_t>(mr->length);
    L_(debug) << " lkey=" << static_cast<uint64_t>(mr->lkey);
    L_(debug) << " rkey=" << static_cast<uint64_t>(mr->rkey);
}

void InputChannelSender::on_addr_resolved(struct rdma_cm_id* id)
{
    IBConnectionGroup<InputChannelConnection>::on_addr_resolved(id);

    if (!mr_data_) {
        // Register memory regions.
        mr_data_ = ibv_reg_mr(
            pd_, const_cast<uint8_t*>(data_source_.data_send_buffer().ptr()),
            data_source_.data_send_buffer().bytes(), IBV_ACCESS_LOCAL_WRITE);
        if (!mr_data_) {
            L_(error) << "ibv_reg_mr failed for mr_data: " << strerror(errno);
            throw InfinibandException("registration of memory region failed");
        }

        mr_desc_ = ibv_reg_mr(pd_, const_cast<fles::MicrosliceDescriptor*>(
                                       data_source_.desc_send_buffer().ptr()),
                              data_source_.desc_send_buffer().bytes(),
                              IBV_ACCESS_LOCAL_WRITE);
        if (!mr_desc_) {
            L_(error) << "ibv_reg_mr failed for mr_desc: " << strerror(errno);
            throw InfinibandException("registration of memory region failed");
        }

        if (true) {
            dump_mr(mr_desc_);
            dump_mr(mr_data_);
        }
    }
}

void InputChannelSender::on_rejected(struct rdma_cm_event* event)
{
    InputChannelConnection* conn =
        static_cast<InputChannelConnection*>(event->id->context);

    conn->on_rejected(event);
    uint_fast16_t i = conn->index();
    conn_.at(i) = nullptr;

    // immediately initiate retry
    std::unique_ptr<InputChannelConnection> connection =
        create_input_node_connection(i);
    connection->connect(compute_hostnames_[i], compute_services_[i]);
    conn_.at(i) = std::move(connection);
}

std::string InputChannelSender::get_state_string()
{
    std::ostringstream s;

    s << "/--- desc buf ---" << std::endl;
    s << "|";
    for (unsigned int i = 0; i < data_source_.desc_buffer().size(); ++i)
        s << " (" << i << ")" << data_source_.desc_buffer().at(i).offset;
    s << std::endl;
    s << "| acked_desc_ = " << acked_desc_ << std::endl;
    s << "/--- data buf ---" << std::endl;
    s << "|";
    for (unsigned int i = 0; i < data_source_.data_buffer().size(); ++i)
        s << " (" << i << ")" << std::hex << data_source_.data_buffer().at(i)
          << std::dec;
    s << std::endl;
    s << "| acked_data_ = " << acked_data_ << std::endl;
    s << "\\---------";

    return s.str();
}

void InputChannelSender::post_send_data(uint64_t timeslice, int cn,
                                        uint64_t mc_offset, uint64_t mc_length,
                                        uint64_t data_offset,
                                        uint64_t data_length, uint64_t skip)
{
    int num_sge = 0;
    struct ibv_sge sge[4];
    // descriptors
    if ((mc_offset & data_source_.desc_send_buffer().size_mask()) <=
        ((mc_offset + mc_length - 1) &
         data_source_.desc_send_buffer().size_mask())) {
        // one chunk
        sge[num_sge].addr = reinterpret_cast<uintptr_t>(
            &data_source_.desc_send_buffer().at(mc_offset));
        sge[num_sge].length = sizeof(fles::MicrosliceDescriptor) * mc_length;
        sge[num_sge++].lkey = mr_desc_->lkey;
    } else {
        // two chunks
        sge[num_sge].addr = reinterpret_cast<uintptr_t>(
            &data_source_.desc_send_buffer().at(mc_offset));
        sge[num_sge].length =
            sizeof(fles::MicrosliceDescriptor) *
            (data_source_.desc_send_buffer().size() -
             (mc_offset & data_source_.desc_send_buffer().size_mask()));
        sge[num_sge++].lkey = mr_desc_->lkey;
        sge[num_sge].addr =
            reinterpret_cast<uintptr_t>(data_source_.desc_send_buffer().ptr());
        sge[num_sge].length =
            sizeof(fles::MicrosliceDescriptor) *
            (mc_length - data_source_.desc_send_buffer().size() +
             (mc_offset & data_source_.desc_send_buffer().size_mask()));
        sge[num_sge++].lkey = mr_desc_->lkey;
    }
    int num_desc_sge = num_sge;
    // data
    if (data_length == 0) {
        // zero chunks
    } else if ((data_offset & data_source_.data_send_buffer().size_mask()) <=
               ((data_offset + data_length - 1) &
                data_source_.data_send_buffer().size_mask())) {
        // one chunk
        sge[num_sge].addr = reinterpret_cast<uintptr_t>(
            &data_source_.data_send_buffer().at(data_offset));
        sge[num_sge].length = data_length;
        sge[num_sge++].lkey = mr_data_->lkey;
    } else {
        // two chunks
        sge[num_sge].addr = reinterpret_cast<uintptr_t>(
            &data_source_.data_send_buffer().at(data_offset));
        sge[num_sge].length =
            data_source_.data_send_buffer().size() -
            (data_offset & data_source_.data_send_buffer().size_mask());
        sge[num_sge++].lkey = mr_data_->lkey;
        sge[num_sge].addr =
            reinterpret_cast<uintptr_t>(data_source_.data_send_buffer().ptr());
        sge[num_sge].length =
            data_length - data_source_.data_send_buffer().size() +
            (data_offset & data_source_.data_send_buffer().size_mask());
        sge[num_sge++].lkey = mr_data_->lkey;
    }
    // copy between buffers
    for (int i = 0; i < num_sge; ++i) {
        if (i < num_desc_sge) {
            data_source_.copy_to_desc_send_buffer(
                reinterpret_cast<fles::MicrosliceDescriptor*>(sge[i].addr) -
                    data_source_.desc_send_buffer().ptr(),
                sge[i].length / sizeof(fles::MicrosliceDescriptor));
        } else {
            data_source_.copy_to_data_send_buffer(
                reinterpret_cast<uint8_t*>(sge[i].addr) -
                    data_source_.data_send_buffer().ptr(),
                sge[i].length);
        }
    }

    conn_[cn]->send_data(sge, num_sge, timeslice, mc_length, data_length, skip);
}

void InputChannelSender::on_completion(const struct ibv_wc& wc)
{
    switch (wc.wr_id & 0xFF) {
    case ID_WRITE_DESC: {
        uint64_t ts = wc.wr_id >> 24;

        int cn = (wc.wr_id >> 8) & 0xFFFF;
        conn_[cn]->on_complete_write();

        uint64_t acked_ts = acked_desc_ / timeslice_size_;
        if (ts == acked_ts)
            do
                ++acked_ts;
            while (ack_.at(acked_ts) > ts);
        else
            ack_.at(ts) = ts;
        acked_data_ =
            data_source_.desc_buffer().at(acked_ts * timeslice_size_).offset;
        acked_desc_ = acked_ts * timeslice_size_;
        if (acked_data_ >= cached_acked_data_ + min_acked_data_ ||
            acked_desc_ >= cached_acked_desc_ + min_acked_desc_) {
            cached_acked_data_ = acked_data_;
            cached_acked_desc_ = acked_desc_;
            data_source_.update_ack_pointers(cached_acked_data_,
                                             cached_acked_desc_);
        }
        if (false) {
            L_(trace) << "[i" << input_index_ << "] "
                      << "write timeslice " << ts
                      << " complete, now: acked_data_=" << acked_data_
                      << " acked_desc_=" << acked_desc_;
        }
    } break;

    case ID_RECEIVE_STATUS: {
        int cn = wc.wr_id >> 8;
        conn_[cn]->on_complete_recv();
        if (conn_[cn]->request_abort_flag()) {
            abort_ = true;
        }
        if (conn_[cn]->done()) {
            ++connections_done_;
            all_done_ = (connections_done_ == conn_.size());
            L_(debug) << "[i" << input_index_ << "] "
                      << "ID_RECEIVE_STATUS final for id " << cn
                      << " all_done=" << all_done_;
        }
    } break;

    case ID_SEND_STATUS: {
    } break;

    default:
        L_(error) << "[i" << input_index_ << "] "
                  << "wc for unknown wr_id=" << (wc.wr_id & 0xFF);
        throw InfinibandException("wc for unknown wr_id");
    }
}