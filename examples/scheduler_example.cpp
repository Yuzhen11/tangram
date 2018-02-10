#include "comm/mailbox.hpp"

namespace xyz {

void Run() {
  Node node{0, "localhost", 32145, false};
  Mailbox mailbox(true, node, 1);
  mailbox.Start();
  //sleep(30);
  mailbox.Stop();
}

}  // namespace xyz

int main() {
  xyz::Run();
}