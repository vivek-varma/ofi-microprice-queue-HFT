#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>
#include <cmath>

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
    if (take_q) { Event e; e.type = EvType::Quote; e.ts = qs[i].ts; e.q = qs[i]; ev.push_back(e); ++i; }
    else        { Event e; e.type = EvType::Trade; e.ts = ts[j].ts; e.t = ts[j]; ev.push_back(e); ++j; }
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
  const double trades_per_year = 60.0 * 252.0;
  return (mean / sd) * std::sqrt(trades_per_year);
}

// RTH for Oct 2023 (EDT=UTC-4): 09:30–16:00 ET => 13:30–20:00 UTC
static inline bool is_rth_utc(TsNanos ts_ns) {
  const long long sec = ts_ns / 1'000'000'000LL;
  const long long sec_in_day = sec % 86400LL;
  return (sec_in_day >= 48600LL) && (sec_in_day < 72000LL);
}

int main(int argc, char** argv) {
  std::string ymd = (argc > 1) ? argv[1] : "20231002";
  const std::string mbp_path = "data/mbp-1/glbx-mdp3-" + ymd + ".mbp-1.dbn.zst";
  const std::string trd_path = "data/trades/glbx-mdp3-" + ymd + ".trades.dbn.zst";

  if (!std::filesystem::exists(mbp_path)) {
    std::cerr << "Missing MBP-1 file: " << mbp_path << "\n"; return 1;
  }
  bool have_trades = std::filesystem::exists(trd_path);
  const std::uint32_t ESZ3_ID = 314863; // ES Dec-2023 in these files

  auto day_q = load_day_from_dbn(mbp_path, "mbp-1", ESZ3_ID);
  DayEvents day_t;
  if (have_trades) day_t = load_day_from_dbn(trd_path, "trades", ESZ3_ID);

  auto ev = merge_streams(day_q.quotes, day_t.trades);

  // --- strategy params (start permissive to avoid 0 trades) ---
  OfiParams P;
  P.tick_size   = 0.25;    // ES
  P.tick_value  = 12.5;    // ES
  P.theta_ofi   = 5.0;     // try 3–8 later
  P.theta_imb   = 0.15;    // try 0.10–0.25 later
  P.slip_ticks  = 1;
  P.max_hold_ns = 2'000'000'000LL;   // 2s

  // Gates (relaxed):
  P.min_spread_ticks       = 1;               // keep 1-tick spread requirement
  P.min_bid_sz             = 2;               // RELAXED (was 5)
  P.min_ask_sz             = 2;               // RELAXED (was 5)
  P.persist_updates        = 3;               // RELAXED (was 3)
  P.min_flip_cooldown_ns   = 120'000'000LL;   // 200ms
  P.rth_only               = true;

  // Assumptions:
  P.fill_at_touch_when_spread1 = true;        // maker-style touch fill
  P.trade_confirm_ns           = 0;           // RELAXED: off for now (was 100ms)

  QueueOfiStrategy strat(P);

  // ---- Diagnostics ----
  std::size_t q_total = 0;
  std::size_t q_rth   = 0;
  std::size_t q_sp1   = 0;
  std::size_t q_sizeok= 0;
  std::size_t sig_nonempty = 0;
  std::size_t fills = 0;

  std::vector<double> trade_pnls; trade_pnls.reserve(2048);
  double running_pnl = 0.0;

  for (const auto& e : ev) {

    if (e.type == EvType::Trade) strat.on_trade(e.t);
    
    if (e.type == EvType::Quote) {
      ++q_total;

      if (P.rth_only && !is_rth_utc(e.ts)) continue;
      ++q_rth;

      // mirror the main structural gates for visibility
      const bool spread_is_one = std::fabs((e.q.ask_px - e.q.bid_px) - P.tick_size) <= 1e-9;
      if (!spread_is_one) continue;
      ++q_sp1;

      if (e.q.bid_sz < P.min_bid_sz || e.q.ask_sz < P.min_ask_sz) continue;
      ++q_sizeok;

      auto sig = strat.on_quote(e.q);
      if (sig.has_value()) ++sig_nonempty;

      const double mid = 0.5 * (e.q.bid_px + e.q.ask_px);
      double realized = strat.act_and_fill(e.ts, mid, sig);
      if (realized != 0.0) { running_pnl += realized; trade_pnls.push_back(realized); ++fills; }
    } else {
      // Optional: feed trades if you later re-enable trade_confirm_ns > 0
      // strat.on_trade(e.t);
    }
  }

  // Close at end-of-day if still in position
  if (strat.pos().side != 0 && !day_q.quotes.empty()) {
    const auto& q = day_q.quotes.back();
    if (!P.rth_only || is_rth_utc(q.ts)) {
      const double mid = 0.5 * (q.bid_px + q.ask_px);
      double realized = strat.act_and_fill(q.ts, mid, 0);
      if (realized != 0.0) { running_pnl += realized; trade_pnls.push_back(realized); ++fills; }
    }
  }

  // stats
  double sharpe = sharpe_annualized(trade_pnls);
  int wins = 0; for (double x : trade_pnls) if (x > 0) ++wins;

  std::cout << std::fixed << std::setprecision(2);
  std::cout << "Trades: " << trade_pnls.size()
            << "  Win%: " << (trade_pnls.empty() ? 0.0 : 100.0*double(wins)/trade_pnls.size())
            << "  PnL: $" << running_pnl
            << "  Sharpe~ " << sharpe << "\n";

  // diagnostics
  std::cout << "[diag] quotes_total=" << q_total
            << " rth=" << q_rth
            << " spread1=" << q_sp1
            << " size_ok=" << q_sizeok
            << " sig_nonempty=" << sig_nonempty
            << " fills=" << fills
            << "\n";

  return 0;
}
