#include "strategy/QueueOfi.hpp"
#include <algorithm>
#include <cmath>

double QueueOfiStrategy::micro() const {
  const double Asz = std::max<QtyI>(1, last_ask_sz);
  const double Bsz = std::max<QtyI>(1, last_bid_sz);
  return (last_ask_px * Bsz + last_bid_px * Asz) / (Asz + Bsz);
}

double QueueOfiStrategy::imbalance_ticks() const {
  const double m = mid();
  return (micro() - m) / P.tick_size;
}

bool QueueOfiStrategy::price_moved(const QuoteL1& q) const {
  return (q.bid_px != last_bid_px) || (q.ask_px != last_ask_px);
}

void QueueOfiStrategy::update_ofi_l1(const QuoteL1& q) {
  // If price moved, queue deltas aren't "flow" at the same level; ignore.
  if (!have_prev || price_moved(q)) {
    ofi_l1 = 0.0;
  } else {
    const double d_bid = static_cast<double>(q.bid_sz) - static_cast<double>(last_bid_sz);
    const double d_ask = static_cast<double>(q.ask_sz) - static_cast<double>(last_ask_sz);
    ofi_l1 += (d_bid - d_ask);
  }
}

int QueueOfiStrategy::desired_position() const {
  const bool long_sig  = (ofi_l1 >  P.theta_ofi) && (imbalance_ticks() >  P.theta_imb);
  const bool short_sig = (ofi_l1 < -P.theta_ofi) && (imbalance_ticks() < -P.theta_imb);
  if (long_sig)  return +1;
  if (short_sig) return -1;
  return 0;
}

std::optional<int> QueueOfiStrategy::on_quote(const QuoteL1& q) {
  // Update signals
  update_ofi_l1(q);

  // Update L1 state AFTER computing OFI vs previous
  last_bid_px = q.bid_px; last_ask_px = q.ask_px;
  last_bid_sz = q.bid_sz; last_ask_sz = q.ask_sz;
  have_prev = true;

  // Return a recommended desired position (+1 / -1 / 0)
  return desired_position();
}

double QueueOfiStrategy::act_and_fill(TsNanos ts, double mid_px, std::optional<int> sig) {
  // Exit due to time
  if (position.side != 0 && ts - position.entry_ts > P.max_hold_ns) {
    // exit at mid with slippage
    double exit = mid_px - position.side * P.slip_ticks * P.tick_size;
    double pnl_ticks = (exit - position.entry_px) / P.tick_size * position.side;
    position = {}; // flat
    return pnl_ticks * P.tick_value;
  }

  // If a signal exists and differs from current side, change position.
  if (sig.has_value() && sig.value() != position.side) {
    // If currently in a position, close it first
    double realized = 0.0;
    if (position.side != 0) {
      double exit = mid_px - position.side * P.slip_ticks * P.tick_size;
      double pnl_ticks = (exit - position.entry_px) / P.tick_size * position.side;
      realized = pnl_ticks * P.tick_value;
      position = {};
    }
    // Open new position if signal is non-zero
    if (sig.value() != 0) {
      position.side = sig.value();
      position.entry_px = mid_px + position.side * P.slip_ticks * P.tick_size;
      position.entry_ts = ts;
    }
    return realized;
  }

  // No realized PnL this event
  return 0.0;
}
