#include "data/DbnReader.hpp"

#include <databento/dbn_file_store.hpp>
#include <databento/record.hpp>
#include <databento/enums.hpp>
#include <databento/pretty.hpp>

#include <chrono>
#include <filesystem>
#include <type_traits>
#include <cmath>
#include <iostream>

// --- time helpers (existing) ---
template <class TP>
static inline TsNanos to_ns(const TP& tp) {
  using namespace std::chrono;
  return static_cast<TsNanos>(duration_cast<nanoseconds>(tp.time_since_epoch()).count());
}

template <class M>
static inline TsNanos get_ts_ns(const M& m) {
  if constexpr (requires { m.ts_event; })         return to_ns(m.ts_event);
  else if constexpr (requires { m.hd.ts_event; }) return to_ns(m.hd.ts_event);
  else if constexpr (requires { m.ts_recv; })     return to_ns(m.ts_recv);
  else if constexpr (requires { m.hd.ts_recv; })  return to_ns(m.hd.ts_recv);
  else return 0;
}

// --- NEW: RTH gate for ES (UTC) 13:30:00–20:00:00 ---
static inline bool is_rth_es_utc(TsNanos ts_ns) {
  constexpr TsNanos DAY_NS   = 24LL * 60 * 60 * 1'000'000'000LL;
  const TsNanos t = (ts_ns % DAY_NS + DAY_NS) % DAY_NS; // nanoseconds since midnight UTC

  constexpr TsNanos RTH_START = (13LL*60 + 30) * 60 * 1'000'000'000LL; // 13:30:00
  constexpr TsNanos RTH_END   = 20LL * 60 * 60 * 1'000'000'000LL;      // 20:00:00
  return (t >= RTH_START) && (t < RTH_END);
}

// --- price conversion helpers (existing) ---
template <class P>
static inline double px_to_double(const P& p) {
  using T = std::decay_t<P>;
  if constexpr (std::is_integral_v<T>) {
    return static_cast<double>(p) * 1e-9;
  } else if constexpr (std::is_floating_point_v<T>) {
    return static_cast<double>(p);
  } else if constexpr (requires { p.mantissa; p.exponent; }) {
    return static_cast<double>(p.mantissa) * std::pow(10.0, static_cast<double>(p.exponent));
  } else if constexpr (requires { p.mant; p.exponent; }) {
    return static_cast<double>(p.mant) * std::pow(10.0, static_cast<double>(p.exponent));
  } else if constexpr (requires { p.mant; p.scale; }) {
    return static_cast<double>(p.mant) * std::pow(10.0, -static_cast<double>(p.scale));
  } else if constexpr (requires { p.raw; p.exponent; }) {
    return static_cast<double>(p.raw) * std::pow(10.0, static_cast<double>(p.exponent));
  } else {
    return 0.0;
  }
}

template <class M> static inline auto  get_bid_px_raw(const M& m) { return m.levels[0].bid_px; }
template <class M> static inline auto  get_ask_px_raw(const M& m) { return m.levels[0].ask_px; }
template <class M> static inline QtyI  get_bid_sz(const M& m)     { return static_cast<QtyI>(m.levels[0].bid_sz); }
template <class M> static inline QtyI  get_ask_sz(const M& m)     { return static_cast<QtyI>(m.levels[0].ask_sz); }
template <class T> static inline auto  get_px_raw(const T& r)     { return r.price; }
template <class T> static inline QtyI  get_sz(const T& r)         { return static_cast<QtyI>(r.size); }

template <class T>
static inline Aggressor get_aggr(const T& r) {
  if constexpr (requires { r.aggressor; }) {
    using E = std::decay_t<decltype(r.aggressor)>;
    if constexpr (requires { E::Buy; E::Sell; }) {
      if (r.aggressor == E::Buy)  return Aggressor::Buy;
      if (r.aggressor == E::Sell) return Aggressor::Sell;
    } else {
      const int v = static_cast<int>(r.aggressor);
      if (v == 1) return Aggressor::Buy;
      if (v == 2) return Aggressor::Sell;
    }
  } else if constexpr (requires { r.side; }) {
    using E = std::decay_t<decltype(r.side)>;
    if constexpr (requires { E::Buy; E::Sell; }) {
      if (r.side == E::Buy)  return Aggressor::Buy;
      if (r.side == E::Sell) return Aggressor::Sell;
    }
  }
  return Aggressor::Unknown;
}

// ---------- unfiltered loader (unchanged) ----------
DayEvents load_day_from_dbn(const std::string& path, const std::string& schema_name) {
  DayEvents out;
  databento::DbnFileStore store{std::filesystem::path{path}};
  store.Replay([&](const databento::Record& rec) -> databento::KeepGoing {
    if (schema_name == "mbp-1") {
      if (const auto* m = rec.GetIf<databento::Mbp1Msg>()) {
        const double bid_px_d = px_to_double(get_bid_px_raw(*m));
        const double ask_px_d = px_to_double(get_ask_px_raw(*m));
        if (bid_px_d <= 0.0 || ask_px_d <= 0.0 || bid_px_d >= ask_px_d) return databento::KeepGoing::Continue;

        QuoteL1 q{};
        q.ts     = get_ts_ns(*m);
        q.bid_px = bid_px_d;
        q.ask_px = ask_px_d;
        q.bid_sz = get_bid_sz(*m);
        q.ask_sz = get_ask_sz(*m);
        out.quotes.push_back(q);
      }
    } else if (schema_name == "trades") {
      if (const auto* t = rec.GetIf<databento::TradeMsg>()) {
        Trade tr{};
        tr.ts   = get_ts_ns(*t);
        tr.px   = px_to_double(get_px_raw(*t));
        tr.sz   = get_sz(*t);
        tr.side = get_aggr(*t);
        out.trades.push_back(tr);
      }
    }
    return databento::KeepGoing::Continue;
  });
  return out;
}

// ---- filtered loader (instrument + RTH) ----
DayEvents load_day_from_dbn(const std::string& path,
                            const std::string& schema_name,
                            std::optional<std::uint32_t> instrument_filter,
                            bool rth_only) {
  DayEvents out;
  databento::DbnFileStore store{std::filesystem::path{path}};

  store.Replay([&](const databento::Record& rec) -> databento::KeepGoing {
    if (schema_name == "mbp-1") {
      if (const auto* m = rec.GetIf<databento::Mbp1Msg>()) {
        if (instrument_filter && m->hd.instrument_id != *instrument_filter)
          return databento::KeepGoing::Continue;

        const TsNanos ts = get_ts_ns(*m);
        if (rth_only && !is_rth_es_utc(ts)) return databento::KeepGoing::Continue;

        const double bid_px_d = px_to_double(get_bid_px_raw(*m));
        const double ask_px_d = px_to_double(get_ask_px_raw(*m));
        if (bid_px_d <= 0.0 || ask_px_d <= 0.0 || bid_px_d >= ask_px_d)
          return databento::KeepGoing::Continue;

        QuoteL1 q{};
        q.ts     = ts;
        q.bid_px = bid_px_d;
        q.ask_px = ask_px_d;
        q.bid_sz = get_bid_sz(*m);
        q.ask_sz = get_ask_sz(*m);
        out.quotes.push_back(q);
      }
    } else if (schema_name == "trades") {
      if (const auto* t = rec.GetIf<databento::TradeMsg>()) {
        if (instrument_filter && t->hd.instrument_id != *instrument_filter)
          return databento::KeepGoing::Continue;

        const TsNanos ts = get_ts_ns(*t);
        if (rth_only && !is_rth_es_utc(ts)) return databento::KeepGoing::Continue;

        Trade tr{};
        tr.ts   = ts;
        tr.px   = px_to_double(get_px_raw(*t));
        tr.sz   = get_sz(*t);
        tr.side = get_aggr(*t);
        out.trades.push_back(tr);
      }
    }
    return databento::KeepGoing::Continue;
  });

  return out;
}
