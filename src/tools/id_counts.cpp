#include <databento/dbn_file_store.hpp>
#include <databento/record.hpp>

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <unordered_map>
#include <vector>

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: id_counts 20231002\n";
    return 1;
  }
  std::string ymd = argv[1];
  std::string mbp_path = "data/mbp-1/glbx-mdp3-" + ymd + ".mbp-1.dbn.zst";
  if (!std::filesystem::exists(mbp_path)) {
    std::cerr << "Missing " << mbp_path << "\n";
    return 1;
  }

  databento::DbnFileStore store{mbp_path};
  std::unordered_map<std::uint32_t, std::uint64_t> counts;

  store.Replay([&](const databento::Record& rec) {
    if (const auto* m = rec.GetIf<databento::Mbp1Msg>()) {
      counts[m->hd.instrument_id]++;
    }
    return databento::KeepGoing::Continue;
  });

  std::vector<std::pair<std::uint32_t, std::uint64_t>> v(counts.begin(), counts.end());
  std::sort(v.begin(), v.end(), [](auto& a, auto& b) { return a.second > b.second; });

  std::cout << "instrument_id,count\n";
  for (auto& [id, c] : v) std::cout << id << "," << c << "\n";
  return 0;
}
