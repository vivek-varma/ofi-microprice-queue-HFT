#pragma once
#include <cstdint>
#include <deque>
#include <optional>
#include "common/Types.hpp"

// A single, minimal position state
struct Position {
  int side = 0;            // +1 long, -1 short, 0 flat
  double entry_px = 0.0;   // entry price (mid with slippage)
  TsNanos entry_ts = 0;    // entry timestamp (ns)
};

struct OfiParams {
  double theta_ofi = 5.0;       // OFI threshold (in contracts)
  double theta_imb = 0.25;      // microprice imbalance (in ticks)
  double tick_size = 0.25;      // product tick (e.g., ES=0.25)
  double tick_value = 12.5;     // $ value per tick (ES=12.5)
  int    slip_ticks = 1;        // slippage ticks per trade side
  std::int64_t max_hold_ns = 2'000'000'000LL; // 2s horizon
};

class QueueOfiStrategy {
 public:
  explicit QueueOfiStrategy(const OfiParams& p): P(p) {}

  // Feed a quote; returns an optional desired position (+1 / -1 / 0)
  std::optional<int> on_quote(const QuoteL1& q);

  // Feed a trade if you want (optional for rules below)
  void on_trade(const Trade& /*t*/) {}

  // Backtester helpers
  double mid() const { return 0.5 * (last_bid_px + last_ask_px); }
  double micro() const;
  double imbalance_ticks() const;     // (micro - mid)/tick_size
  double ofi() const { return ofi_l1; }

  // Simple one-position backtest interface
  // Returns realized PnL in dollars when a trade exits; otherwise 0
  double act_and_fill(TsNanos ts, double mid_px, std::optional<int> sig);

  const Position& pos() const { return position; }

 private:
  OfiParams P;

  // L1 state
  double last_bid_px = 0.0, last_ask_px = 0.0;
  QtyI   last_bid_sz = 0,   last_ask_sz = 0;
  bool   have_prev = false;

  // Signals
  double ofi_l1 = 0.0;

  // Position
  Position position{};

  // helpers
  void update_ofi_l1(const QuoteL1& q);
  bool price_moved(const QuoteL1& q) const;
  int  desired_position() const; // +1 long, -1 short, 0 flat
};
