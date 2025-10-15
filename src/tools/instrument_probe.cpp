#include <databento/dbn_file_store.hpp>
#include <databento/record.hpp>
#include <iostream>
#include <set>
#include <string>

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: instrument_probe <path-to-dbn.zst>\n";
    return 1;
  }
  std::string path = argv[1];
  databento::DbnFileStore s{path};
  std::set<std::uint32_t> ids;

  s.Replay([&](const databento::Record& r) {
    if (auto* m = r.GetIf<databento::Mbp1Msg>()) ids.insert(m->hd.instrument_id);
    if (auto* t = r.GetIf<databento::TradeMsg>()) ids.insert(t->hd.instrument_id);
    return databento::KeepGoing::Continue;
  });

  std::cout << "Unique instrument_ids: " << ids.size() << "\n";
  for (auto id : ids) std::cout << id << "\n";
  return 0;
}
