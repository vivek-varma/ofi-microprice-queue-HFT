#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#include<cmath>

#include "common/Types.hpp"
#include "data/DbnReader.hpp"
#include "strategy/QueueOfi.hpp"

// --- merge, same as in smoke.cpp ---
static std::vector<Event> merge_streams(const std::vector<QuoteL1>& qs,
                                        const std::vector<Trade>& ts) {
  std::vector<Event> ev;
  ev.reserve(qs.size() + ts.size());
  size_t i = 0, j = 0;
  while (i < qs.size() || j < ts.size()) {
    bool take_q = (j == ts.size()) || (i < qs.size() && qs[i].ts <= ts[j].ts);
    if (take_q) {
      Event e; e.type = EvType::Quote; e.ts = qs[i].ts; e.q = qs[i]; ev.push_back(e); ++i;
    } else {
      Event e; e.type = EvType::Trade; e.ts = ts[j].ts; e.t = ts[j]; ev.push_back(e); ++j;
    }
  }
  return ev;
}

static double sharpe_annualized(const std::vector<double>& rets) {
  if (rets.size() < 2) return 0.0;
  double mean = std::accumulate(rets.begin(), rets.end(), 0.0) / rets.size();
  double var = 0.0;
  for (double r : rets) var += (r - mean) * (r - mean);
  var /= (rets.size() - 1);
  double sd = std::sqrt(std::max(1e-12, var));
  // Treat each PnL as a per-trade return proxy; assume ~60 trades/day, ~252 days
  const double trades_per_year = 60.0 * 252.0;
  return (mean / sd) * std::sqrt(trades_per_year);
}

int main(int argc, char** argv) {
  std::string ymd = (argc > 1) ? argv[1] : "20231002";
  const std::string mbp_path = "data/mbp-1/glbx-mdp3-" + ymd + ".mbp-1.dbn.zst";
  const std::string trd_path = "data/trades/glbx-mdp3-" + ymd + ".trades.dbn.zst";

  if (!std::filesystem::exists(mbp_path)) {
    std::cerr << "Missing MBP-1 file: " << mbp_path << "\n"; return 1;
  }
  bool have_trades = std::filesystem::exists(trd_path);

  auto day_q = load_day_from_dbn(mbp_path, "mbp-1");
  DayEvents day_t;
  if (have_trades) day_t = load_day_from_dbn(trd_path, "trades");

  auto ev = merge_streams(day_q.quotes, day_t.trades);

  // --- strategy + backtest params (tweak for your product) ---
  OfiParams P;
  P.tick_size  = 0.25;    // ES
  P.tick_value = 12.5;    // ES
  P.theta_ofi  = 5.0;     // contracts
  P.theta_imb  = 0.25;    // ticks
  P.slip_ticks = 1;
  P.max_hold_ns = 2'000'000'000LL;

  QueueOfiStrategy strat(P);

  std::vector<double> trade_pnls; trade_pnls.reserve(256);
  double running_pnl = 0.0;

  for (const auto& e : ev) {
    if (e.type == EvType::Quote) {
      auto sig = strat.on_quote(e.q);         // update state & get desired position
      double realized = strat.act_and_fill(e.ts, 0.5*(e.q.bid_px + e.q.ask_px), sig);
      if (realized != 0.0) { running_pnl += realized; trade_pnls.push_back(realized); }
    } else {
      // Optional: strat.on_trade(e.t);
    }
  }

  // Close at end-of-day if still in position
  if (strat.pos().side != 0 && !day_q.quotes.empty()) {
    const auto& q = day_q.quotes.back();
    double realized = strat.act_and_fill(q.ts, 0.5*(q.bid_px + q.ask_px), 0);
    if (realized != 0.0) { running_pnl += realized; trade_pnls.push_back(realized); }
  }

  // stats
  double sharpe = sharpe_annualized(trade_pnls);
  int wins = 0; for (double x : trade_pnls) if (x > 0) ++wins;

  std::cout << std::fixed << std::setprecision(2);
  std::cout << "Trades: " << trade_pnls.size()
            << "  Win%: " << (trade_pnls.empty() ? 0.0 : 100.0*double(wins)/trade_pnls.size())
            << "  PnL: $" << running_pnl
            << "  Sharpe~ " << sharpe << "\n";

  return 0;
}
