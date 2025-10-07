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
  // treat each realized PnL as a "trade return proxy"; scale to yearly trades
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
    int wins=0; for (double x: trade_pnls) if (x>0) ++wins;
    return 100.0*double(wins)/trade_pnls.size();
  }
};

static bool load_day(const std::string& ymd, DayEvents& out) {
  const std::string mbp_path = "data/mbp-1/glbx-mdp3-" + ymd + ".mbp-1.dbn.zst";
  const std::string trd_path = "data/trades/glbx-mdp3-" + ymd + ".trades.dbn.zst";
  if (!std::filesystem::exists(mbp_path)) return false;

  auto day_q = load_day_from_dbn(mbp_path, "mbp-1");
  DayEvents day_t;
  if (std::filesystem::exists(trd_path)) day_t = load_day_from_dbn(trd_path, "trades");

  auto ev = merge_streams(day_q.quotes, day_t.trades);
  out.quotes = std::move(day_q.quotes);
  out.trades = std::move(day_t.trades);
  // we’ll iterate over merged in the backtest function
  return true;
}

static RunStats run_one_day(const std::string& ymd, const OfiParams& P) {
  RunStats rs;

  const std::string mbp_path = "data/mbp-1/glbx-mdp3-" + ymd + ".mbp-1.dbn.zst";
  const std::string trd_path = "data/trades/glbx-mdp3-" + ymd + ".trades.dbn.zst";
  if (!std::filesystem::exists(mbp_path)) return rs;

  auto day_q = load_day_from_dbn(mbp_path, "mbp-1");
  DayEvents day_t;
  if (std::filesystem::exists(trd_path)) day_t = load_day_from_dbn(trd_path, "trades");

  auto ev = merge_streams(day_q.quotes, day_t.trades);

  QueueOfiStrategy strat(P);
  for (const auto& e : ev) {
    if (e.type == EvType::Quote) {
      auto sig = strat.on_quote(e.q);
      double realized = strat.act_and_fill(e.ts, 0.5*(e.q.bid_px + e.q.ask_px), sig);
      if (realized != 0.0) { rs.pnl += realized; rs.trade_pnls.push_back(realized); }
    } else {
      // optional: strat.on_trade(e.t);
    }
  }
  if (strat.pos().side != 0 && !day_q.quotes.empty()) {
    const auto& q = day_q.quotes.back();
    double realized = strat.act_and_fill(q.ts, 0.5*(q.bid_px + q.ask_px), 0);
    if (realized != 0.0) { rs.pnl += realized; rs.trade_pnls.push_back(realized); }
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

int main(int , char** ) {
  // --- grids (adjust as needed) ---
  std::vector<double> grid_ofi  = {3.0, 5.0, 8.0, 12.0};
  std::vector<double> grid_imb  = {0.10, 0.25, 0.40};
  std::vector<int>    grid_slip = {1, 2};
  std::vector<std::int64_t> grid_hold = {
    1'000'000'000LL,    // 1s
    2'000'000'000LL,    // 2s
    3'000'000'000LL     // 3s
  };

  // product constants (ES by default)
  const double TICK_SIZE  = 0.25;
  const double TICK_VALUE = 12.5;

  // TRAIN and VALIDATION ranges
  auto train_days = ymd_range_202310(1, 15);
  auto valid_days = ymd_range_202310(16, 30);

  // --- sweep on TRAIN ---
  struct Score { double sharpe; double pnl; size_t trades; ParamCombo pc; };
  Score best{ -1e9, 0.0, 0, {0,0,0,0} };

  std::cout << std::fixed << std::setprecision(2);

  for (double th_ofi : grid_ofi)
  for (double th_imb : grid_imb)
  for (int slip : grid_slip)
  for (auto hold_ns : grid_hold) {
    OfiParams P;
    P.tick_size  = TICK_SIZE;
    P.tick_value = TICK_VALUE;
    P.theta_ofi  = th_ofi;
    P.theta_imb  = th_imb;
    P.slip_ticks = slip;
    P.max_hold_ns = hold_ns;

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

  // --- evaluate on VALIDATION ---
  OfiParams Pbest;
  Pbest.tick_size  = TICK_SIZE;
  Pbest.tick_value = TICK_VALUE;
  Pbest.theta_ofi  = best.pc.theta_ofi;
  Pbest.theta_imb  = best.pc.theta_imb;
  Pbest.slip_ticks = best.pc.slip_ticks;
  Pbest.max_hold_ns = best.pc.max_hold_ns;

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
