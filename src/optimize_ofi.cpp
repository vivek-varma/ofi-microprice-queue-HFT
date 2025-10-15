#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <tuple>
#include <vector>
#include <cmath>

#include "common/Types.hpp"
#include "data/DbnReader.hpp"
#include "strategy/QueueOfi.hpp"

// ---------- Utilities ----------
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

struct RunStats {
  double pnl = 0.0;
  std::vector<double> trade_pnls;
  size_t trades() const { return trade_pnls.size(); }
  double sharpe() const { return sharpe_annualized(trade_pnls); }
  double winrate() const {
    if (trade_pnls.empty()) return 0.0;
    int wins=0; for (double x : trade_pnls) if (x > 0) ++wins;
    return 100.0 * double(wins) / trade_pnls.size();
  }
};

// --- RTH for Oct 2023 (EDT=UTC-4): 09:30–16:00 ET => 13:30–20:00 UTC
static inline bool is_rth_utc(TsNanos ts_ns) {
  const long long sec = ts_ns / 1'000'000'000LL;
  const long long sec_in_day = sec % 86400LL;
  return (sec_in_day >= 48600LL) && (sec_in_day < 72000LL);
}

static RunStats run_one_day(const std::string& ymd, const OfiParams& P) {
  RunStats rs;

  const std::string mbp_path = "data/mbp-1/glbx-mdp3-" + ymd + ".mbp-1.dbn.zst";
  const std::string trd_path = "data/trades/glbx-mdp3-" + ymd + ".trades.dbn.zst";
  if (!std::filesystem::exists(mbp_path)) return rs;

  const std::uint32_t ESZ3_ID = 314863;

  auto day_q = load_day_from_dbn(mbp_path, "mbp-1", ESZ3_ID);
  DayEvents day_t;
  if (std::filesystem::exists(trd_path)) day_t = load_day_from_dbn(trd_path, "trades", ESZ3_ID);

  auto ev = merge_streams(day_q.quotes, day_t.trades);

  QueueOfiStrategy strat(P);

  for (const auto& e : ev) {
    if (e.type == EvType::Trade) {
      strat.on_trade(e.t);               // keep identical to backtest
      continue;                          // no direct action on trades
    }

    const auto& q = e.q;

    // --- SAME GATES AS BACKTEST (keep order identical) ---
    if (P.rth_only && !is_rth_utc(e.ts)) continue;

    // spread == 1 tick
    const double spr = q.ask_px - q.bid_px;
    if (P.min_spread_ticks > 0) {
      const double need = P.min_spread_ticks * P.tick_size;
      if (std::fabs(spr - need) > 1e-9) continue;
    }

    // min sizes
    if (q.bid_sz < P.min_bid_sz || q.ask_sz < P.min_ask_sz) continue;

    // signal & execution
    auto sig = strat.on_quote(q);
    const double mid = 0.5 * (q.bid_px + q.ask_px);
    double realized = strat.act_and_fill(e.ts, mid, sig);
    if (realized != 0.0) { rs.pnl += realized; rs.trade_pnls.push_back(realized); }
  }

  // EOD flatten
  if (strat.pos().side != 0 && !day_q.quotes.empty()) {
    const auto& q = day_q.quotes.back();
    if (!P.rth_only || is_rth_utc(q.ts)) {
      const double mid = 0.5 * (q.bid_px + q.ask_px);
      double realized = strat.act_and_fill(q.ts, mid, 0);
      if (realized != 0.0) { rs.pnl += realized; rs.trade_pnls.push_back(realized); }
    }
  }

  return rs;
}

static std::vector<std::string> ymd_range_202310(int d0, int d1) {
  std::vector<std::string> v;
  for (int d=d0; d<=d1; ++d) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "202310%02d", d);
    v.emplace_back(buf);
  }
  return v;
}

struct ParamCombo {
  double theta_ofi;
  double theta_imb;
  int    slip_ticks;
  std::int64_t max_hold_ns;
};

int main(int, char**) {
  // --- small, safe grid (same ballpark as backtest that produced trades) ---
  std::vector<double> grid_ofi  = {5.0, 6.0};         // keep near 5
  std::vector<double> grid_imb  = {0.10, 0.15};       // near 0.15
  std::vector<int>    grid_slip = {1};
  std::vector<std::int64_t> grid_hold = {
    1'000'000'000LL,                                  // 1s
    2'000'000'000LL                                   // 2s
  };

  // product constants (ES)
  const double TICK_SIZE  = 0.25;
  const double TICK_VALUE = 12.5;

  // TRAIN and VALIDATION ranges
  auto train_days = ymd_range_202310(1, 15);
  auto valid_days = ymd_range_202310(16, 30);

  struct Score { double sharpe; double pnl; size_t trades; ParamCombo pc; };
  Score best{ -1e9, 0.0, 0, {0,0,0,0} };

  std::cout << std::fixed << std::setprecision(2);

  for (double th_ofi : grid_ofi)
  for (double th_imb : grid_imb)
  for (int slip : grid_slip)
  for (auto hold_ns : grid_hold) {
    OfiParams P;
    P.tick_size   = TICK_SIZE;
    P.tick_value  = TICK_VALUE;
    P.theta_ofi   = th_ofi;
    P.theta_imb   = th_imb;
    P.slip_ticks  = slip;
    P.max_hold_ns = hold_ns;

    // --- gates/assumptions: EXACT match to working backtest ---
    P.min_spread_ticks       = 1;
    P.min_bid_sz             = 2;
    P.min_ask_sz             = 2;
    P.persist_updates        = 3;
    P.min_flip_cooldown_ns   = 120'000'000LL;   // 120ms
    P.rth_only               = true;

    // Assumptions you used in backtest:
    P.fill_at_touch_when_spread1 = true;
    P.trade_confirm_ns           = 0;           // disabled

    RunStats agg{};
    size_t days_used = 0;
    for (const auto& ymd : train_days) {
      if (!std::filesystem::exists("data/mbp-1/glbx-mdp3-" + ymd + ".mbp-1.dbn.zst"))
        continue;
      RunStats rs = run_one_day(ymd, P);
      agg.pnl += rs.pnl;
      agg.trade_pnls.insert(agg.trade_pnls.end(), rs.trade_pnls.begin(), rs.trade_pnls.end());
      ++days_used;
    }
    if (days_used == 0) continue;

    double s = agg.sharpe();
    if (s > best.sharpe) {
      best = { s, agg.pnl, agg.trades(), {th_ofi, th_imb, slip, hold_ns} };
    }

    std::cout << "[TRAIN] ofi=" << th_ofi
              << " imb=" << th_imb
              << " slip=" << slip
              << " hold=" << (hold_ns/1e9) << "s"
              << " | trades=" << agg.trades()
              << " pnl=$" << agg.pnl
              << " sharpe=" << s
              << "\n";
  }

  if (best.sharpe <= -1e8) {
    std::cerr << "No training days found on disk. Make sure Oct 1–15 files exist.\n";
    return 1;
  }

  std::cout << "\n=== BEST ON TRAIN ===\n"
            << "ofi=" << best.pc.theta_ofi
            << " imb=" << best.pc.theta_imb
            << " slip=" << best.pc.slip_ticks
            << " hold=" << (best.pc.max_hold_ns/1e9) << "s"
            << " | trades=" << best.trades
            << " pnl=$" << best.pnl
            << " sharpe=" << best.sharpe
            << "\n\n";

  // --- VALIDATION ---
  OfiParams Pbest;
  Pbest.tick_size   = TICK_SIZE;
  Pbest.tick_value  = TICK_VALUE;
  Pbest.theta_ofi   = best.pc.theta_ofi;
  Pbest.theta_imb   = best.pc.theta_imb;
  Pbest.slip_ticks  = best.pc.slip_ticks;
  Pbest.max_hold_ns = best.pc.max_hold_ns;

  Pbest.min_spread_ticks       = 1;
  Pbest.min_bid_sz             = 2;
  Pbest.min_ask_sz             = 2;
  Pbest.persist_updates        = 3;
  Pbest.min_flip_cooldown_ns   = 120'000'000LL;
  Pbest.rth_only               = true;
  Pbest.fill_at_touch_when_spread1 = true;
  Pbest.trade_confirm_ns           = 0;

  RunStats vagg{};
  size_t vdays_used = 0;
  for (const auto& ymd : valid_days) {
    if (!std::filesystem::exists("data/mbp-1/glbx-mdp3-" + ymd + ".mbp-1.dbn.zst"))
      continue;
    RunStats rs = run_one_day(ymd, Pbest);
    vagg.pnl += rs.pnl;
    vagg.trade_pnls.insert(vagg.trade_pnls.end(), rs.trade_pnls.begin(), rs.trade_pnls.end());
    ++vdays_used;
  }

  std::cout << "=== VALIDATION (Oct 16–30) ===\n"
            << "days=" << vdays_used
            << " trades=" << vagg.trades()
            << " pnl=$" << vagg.pnl
            << " sharpe=" << vagg.sharpe()
            << " win%=" << vagg.winrate()
            << "\n";

  return 0;
}
