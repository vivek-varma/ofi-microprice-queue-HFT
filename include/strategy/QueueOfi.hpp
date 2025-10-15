#pragma once
#include <cstdint>
#include <deque>
#include <optional>
#include "common/Types.hpp"

struct Position {
  int side = 0;
  double entry_px = 0.0;
  TsNanos entry_ts = 0;
};

struct OfiParams {
  double theta_ofi = 5.0;
  double theta_imb = 0.25;      // normalized imbalance ([-1,1])
  double tick_size = 0.25;
  double tick_value = 12.5;
  int    slip_ticks = 1;
  std::int64_t max_hold_ns = 2'000'000'000LL;

  // microstructure / debounce
  int   min_spread_ticks = 1;
  QtyI  min_bid_sz       = 2;
  QtyI  min_ask_sz       = 2;
  int   persist_updates  = 2;
  std::int64_t min_flip_cooldown_ns = 50'000'000LL;
  bool  rth_only         = true;

  // NEW: small & defensible assumptions
  bool          fill_at_touch_when_spread1 = true;     // maker-style touch fill when spread==1
  std::int64_t  trade_confirm_ns           = 100'000'000LL; // require confirming trade within 100ms
};

class QueueOfiStrategy {
 public:
  explicit QueueOfiStrategy(const OfiParams& p): P(p) {}

  std::optional<int> on_quote(const QuoteL1& q);
  void on_trade(const Trade& t);   // now used for confirmation

  double mid() const { return 0.5 * (last_bid_px + last_ask_px); }
  double micro() const;
  double imbalance_ticks() const;
  double ofi() const { return ofi_l1; }

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

  // Trade confirmation
  TsNanos last_trade_ts = 0;
  int     last_trade_dir = 0; // +1 buy-agg, -1 sell-agg

  // Position
  Position position{};

  void update_ofi_l1(const QuoteL1& q);
  bool price_moved(const QuoteL1& q) const;
  int  desired_position() const; // +1 / -1 / 0
};
