#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "common/Types.hpp"
#include "data/DbnReader.hpp"

static double microprice(const QuoteL1& q) {
  const double A = q.ask_px;                // PriceI is double now
  const double B = q.bid_px;
  const double Asz = std::max<QtyI>(1, q.ask_sz);
  const double Bsz = std::max<QtyI>(1, q.bid_sz);
  return (A * Bsz + B * Asz) / (Asz + Bsz);
}

static std::vector<Event> merge_streams(const std::vector<QuoteL1>& qs,
                                        const std::vector<Trade>& ts) {
  std::vector<Event> ev;
  ev.reserve(qs.size() + ts.size());
  size_t i = 0, j = 0;
  while (i < qs.size() || j < ts.size()) {
    const bool take_q = (j == ts.size()) || (i < qs.size() && qs[i].ts <= ts[j].ts);
    if (take_q) {
      Event e; e.type = EvType::Quote; e.ts = qs[i].ts; e.q = qs[i]; ev.push_back(e); ++i;
    } else {
      Event e; e.type = EvType::Trade; e.ts = ts[j].ts; e.t = ts[j]; ev.push_back(e); ++j;
    }
  }
  return ev;
}

int main(int argc, char** argv) {
  // Auto-pick a day you likely have (override via argv[1])
  std::string ymd = (argc > 1) ? argv[1] : "20231002";
  std::string mbp_path = "data/mbp-1/glbx-mdp3-" + ymd + ".mbp-1.dbn.zst";
  std::string trd_path = "data/trades/glbx-mdp3-" + ymd + ".trades.dbn.zst";

  if (!std::filesystem::exists(mbp_path)) {
    std::cerr << "Missing MBP-1 file: " << mbp_path << "\n";
    return 1;
  }
  const bool have_trades = std::filesystem::exists(trd_path);

  std::cout << "Loading MBP-1: " << mbp_path << "\n";
  auto day_q = load_day_from_dbn(mbp_path, "mbp-1");

  std::cout << "Loading Trades: " << trd_path
            << (have_trades ? "" : " (not found, skipping)") << "\n";
  DayEvents day_t;
  if (have_trades) day_t = load_day_from_dbn(trd_path, "trades");

  std::cout << "quotes: " << day_q.quotes.size()
            << ", trades: " << day_t.trades.size() << "\n";

  // Merge and compute a couple of sanity checks
  auto ev = merge_streams(day_q.quotes, day_t.trades);
  std::cout << "merged events: " << ev.size() << "\n";

  // sample microprice + simple OFI(L1) running sum
  std::int64_t ofi_sum = 0;
  QtyI last_bid_sz = 0, last_ask_sz = 0;
  bool first_sz = true;
  std::size_t sampled = 0;

  std::cout << std::fixed << std::setprecision(2);

  for (const auto& e : ev) {
    if (e.type == EvType::Quote) {
      const double mp = microprice(e.q);
      if (sampled < 3) {
        std::cout << "ts=" << e.ts
                  << " bid=" << e.q.bid_px << "x" << e.q.bid_sz
                  << " ask=" << e.q.ask_px << "x" << e.q.ask_sz
                  << " micro=" << mp << "\n";
        ++sampled;
      }
      if (first_sz) {
        last_bid_sz = e.q.bid_sz; last_ask_sz = e.q.ask_sz; first_sz = false;
      }
      // OFI-L1 instant (toy): Δbid_sz + Δask_sz
      ofi_sum += (static_cast<std::int64_t>(e.q.bid_sz) - last_bid_sz)
               + (static_cast<std::int64_t>(e.q.ask_sz) - last_ask_sz);
      last_bid_sz = e.q.bid_sz; last_ask_sz = e.q.ask_sz;
    }
  }
  std::cout << "OFI(L1) running sum ~ " << ofi_sum << "\n";
  std::cout << "Smoke test OK.\n";
  return 0;
}
