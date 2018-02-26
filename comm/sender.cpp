#include "comm/sender.hpp"

namespace xyz {

void Sender::Send(Message msg) { GetWorkQueue()->Push(std::move(msg)); }

void Sender::Process(Message msg) { mailbox_->Send(msg); }

} // namespace xyz
