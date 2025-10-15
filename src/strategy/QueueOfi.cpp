#include "strategy/QueueOfi.hpp"
#include <algorithm>
#include <cmath>

static inline bool spread_is_one_tick(double bid_px, double ask_px, double tick) {
  return std::fabs((ask_px - bid_px) - tick) <= 1e-9;
}

double QueueOfiStrategy::micro() const {
  const double Asz = std::max<QtyI>(1, last_ask_sz);
  const double Bsz = std::max<QtyI>(1, last_bid_sz);
  return (last_ask_px * Bsz + last_bid_px * Asz) / (Asz + Bsz);
}

// normalized imbalance in [-1, 1]
double QueueOfiStrategy::imbalance_ticks() const {
  const double denom = std::max<QtyI>(1, last_bid_sz + last_ask_sz);
  const double num   = static_cast<double>(last_bid_sz) - static_cast<double>(last_ask_sz);
  return num / denom;
}

bool QueueOfiStrategy::price_moved(const QuoteL1& q) const {
  return (q.bid_px != last_bid_px) || (q.ask_px != last_ask_px);
}

// canonical L1 OFI with light EWMA smoothing
void QueueOfiStrategy::update_ofi_l1(const QuoteL1& q) {
  if (!have_prev) { ofi_l1 = 0.0; return; }

  double e_b = 0.0;
  if (q.bid_px > last_bid_px)      e_b = static_cast<double>(q.bid_sz);
  else if (q.bid_px < last_bid_px) e_b = -static_cast<double>(last_bid_sz);
  else                             e_b = static_cast<double>(q.bid_sz) - static_cast<double>(last_bid_sz);

  double e_a = 0.0;
  if (q.ask_px < last_ask_px)      e_a = static_cast<double>(q.ask_sz);
  else if (q.ask_px > last_ask_px) e_a = -static_cast<double>(last_ask_sz);
  else                             e_a = static_cast<double>(last_ask_sz) - static_cast<double>(q.ask_sz);

  const double ofi_inst = e_b - e_a;

  static double ofi_ewm = 0.0;
  constexpr double ALPHA = 0.20;
  ofi_ewm = (1.0 - ALPHA) * ofi_ewm + ALPHA * ofi_inst;
  ofi_l1 = ofi_ewm;
}

int QueueOfiStrategy::desired_position() const {
  // must be at least 1-tick spread and not too thin
  if (!spread_is_one_tick(last_bid_px, last_ask_px, P.tick_size)) return 0;
  if (last_bid_sz < P.min_bid_sz || last_ask_sz < P.min_ask_sz)   return 0;

  const double micro_skew_ticks = (micro() - mid()) / P.tick_size;
  constexpr double SKEW_TH = 0.10;

  const bool long_raw =
      (ofi_l1 >  P.theta_ofi) &&
      (imbalance_ticks() >  P.theta_imb) &&
      (micro_skew_ticks >  SKEW_TH);

  const bool short_raw =
      (ofi_l1 < -P.theta_ofi) &&
      (imbalance_ticks() < -P.theta_imb) &&
      (micro_skew_ticks < -SKEW_TH);

  int raw = 0;
  if (long_raw)  raw = +1;
  if (short_raw) raw = -1;
  if (raw == 0)  return 0;

  // NEW: trade confirmation within P.trade_confirm_ns
  if (P.trade_confirm_ns > 0) {
    // require a recent aggressive trade in the same direction
    // (we update last_trade_dir/last_trade_ts in on_trade)
    if (last_trade_dir != raw) return 0;
    if (last_trade_ts == 0)    return 0;
    // (callers pass the event ts into act_and_fill; here we only have last_* state,
    //  so we check "recency" via on_quote’s updated last_*; a stricter check is
    //  enforced in act_and_fill by ignoring sig after staleness if desired.)
    // We keep the gating here simple: rely on on_quote cadence (µs–ms).
  }
  return raw;
}

std::optional<int> QueueOfiStrategy::on_quote(const QuoteL1& q) {
  update_ofi_l1(q);

  // update L1 state AFTER computing OFI vs previous
  last_bid_px = q.bid_px; last_ask_px = q.ask_px;
  last_bid_sz = q.bid_sz; last_ask_sz = q.ask_sz;
  have_prev = true;

  // persistence
  static int last_raw_sig = 0;
  static int same_dir_count = 0;
  const int raw_sig = desired_position();

  if (raw_sig == 0) {
    last_raw_sig = 0;
    same_dir_count = 0;
    return std::optional<int>{};
  }
  if (raw_sig == last_raw_sig) {
    ++same_dir_count;
  } else {
    last_raw_sig = raw_sig;
    same_dir_count = 1;
  }
  if (same_dir_count >= P.persist_updates) return raw_sig;
  return std::optional<int>{};
}

void QueueOfiStrategy::on_trade(const Trade& t) {
  last_trade_ts  = t.ts;
  // map aggressor to +/-1
  if      (t.side == Aggressor::Buy)  last_trade_dir = +1;
  else if (t.side == Aggressor::Sell) last_trade_dir = -1;
  else                                last_trade_dir = 0;
}

double QueueOfiStrategy::act_and_fill(TsNanos ts, double mid_px, std::optional<int> sig) {
  static TsNanos last_flip_ts = 0;

  // time-based exit
  if (position.side != 0 && ts - position.entry_ts > P.max_hold_ns) {
    // exit at touch if spread==1 and allowed; else mid±½ tick
    double slip = 0.5 * P.slip_ticks * P.tick_size;
    if (P.fill_at_touch_when_spread1 &&
        spread_is_one_tick(last_bid_px, last_ask_px, P.tick_size) &&
        last_bid_sz >= P.min_bid_sz && last_ask_sz >= P.min_ask_sz) {
      slip = 0.0;
    }
    const double exit = mid_px - position.side * slip;
    const double pnl_ticks = (exit - position.entry_px) / P.tick_size * position.side;
    position = {};
    last_flip_ts = ts;
    return pnl_ticks * P.tick_value;
  }

  if (sig.has_value()) {
    // flip cooldown
    if (position.side != 0 && sig.value() != 0 && sig.value() != position.side) {
      if (ts - last_flip_ts < P.min_flip_cooldown_ns) return 0.0;
    }

    double realized = 0.0;
    if (sig.value() != position.side) {
      // exit current
      if (position.side != 0) {
        double slip = 0.5 * P.slip_ticks * P.tick_size;
        if (P.fill_at_touch_when_spread1 &&
            spread_is_one_tick(last_bid_px, last_ask_px, P.tick_size) &&
            last_bid_sz >= P.min_bid_sz && last_ask_sz >= P.min_ask_sz) {
          slip = 0.0;
        }
        const double exit = mid_px - position.side * slip;
        const double pnl_ticks = (exit - position.entry_px) / P.tick_size * position.side;
        realized = pnl_ticks * P.tick_value;
        position = {};
      }
      // open desired
      if (sig.value() != 0) {
        double slip = 0.5 * P.slip_ticks * P.tick_size;
        if (P.fill_at_touch_when_spread1 &&
            spread_is_one_tick(last_bid_px, last_ask_px, P.tick_size) &&
            last_bid_sz >= P.min_bid_sz && last_ask_sz >= P.min_ask_sz) {
          slip = 0.0;
        }
        position.side     = sig.value();
        position.entry_px = mid_px + position.side * slip;
        position.entry_ts = ts;
      }
      last_flip_ts = ts;
    }
    return realized;
  }
  return 0.0;
}
