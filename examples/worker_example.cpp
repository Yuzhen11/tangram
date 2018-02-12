#include "comm/mailbox.hpp"
#include "base/third_party/network_utils.h"

namespace xyz {

  void Run() {
    Node node{0, "localhost", 32145, false};
    Mailbox mailbox(false, node, 1);
    mailbox.Start();

    Message msg;
    msg.meta.sender = 999;
    msg.meta.recver = 0;
    msg.meta.flag = Flag::kOthers;
    third_party::SArray<int> keys{1};
    third_party::SArray<float> vals{0.4};
    msg.AddData(keys);
    msg.AddData(vals);
    mailbox.Send(msg);

    mailbox.Stop();
  }

}  // namespace xyz

int main(int argc, char** argv) {
  xyz::Run();
}
