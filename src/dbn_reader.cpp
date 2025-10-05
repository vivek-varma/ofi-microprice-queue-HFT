#include "data/DbnReader.hpp"

#include <databento/dbn_file_store.hpp>
#include <databento/record.hpp>
#include <databento/enums.hpp>
#include <databento/pretty.hpp>  // optional (debug prints)

#include <chrono>
#include <filesystem>
#include <type_traits>
#include <cmath>
#include <iostream>

// ---------- time conversion ----------
template <class TP>
static inline TsNanos to_ns(const TP& tp) {
  using namespace std::chrono;
  return static_cast<TsNanos>(duration_cast<nanoseconds>(tp.time_since_epoch()).count());
}

template <class M>
static inline TsNanos get_ts_ns(const M& m) {
  if constexpr (requires { m.ts_recv; })          return to_ns(m.ts_recv);
  else if constexpr (requires { m.ts_event; })     return to_ns(m.ts_event);
  else if constexpr (requires { m.hd.ts_recv; })   return to_ns(m.hd.ts_recv);
  else if constexpr (requires { m.hd.ts_event; })  return to_ns(m.hd.ts_event);
  else return 0;
}

// ---------- FixedPrice (or numeric) → double ----------
template <class P>
static inline double px_to_double(const P& p) {
  using T = std::decay_t<P>;

  // Databento GLBX often encodes price as integer *nano-dollars* (×1e9).
  // If it's an integral, interpret as nanos and scale down.
  if constexpr (std::is_integral_v<T>) {
    return static_cast<double>(p) * 1e-9;   // nanos → dollars
  }
  // If it's already a floating type, just cast.
  else if constexpr (std::is_floating_point_v<T>) {
    return static_cast<double>(p);
  }
  // Common fixed-price layouts in headers:
  // value = mantissa * 10^exponent
  else if constexpr (requires { p.mantissa; p.exponent; }) {
    return static_cast<double>(p.mantissa) * std::pow(10.0, static_cast<double>(p.exponent));
  }
  // value = mant * 10^exponent
  else if constexpr (requires { p.mant; p.exponent; }) {
    return static_cast<double>(p.mant) * std::pow(10.0, static_cast<double>(p.exponent));
  }
  // value = mant * 10^-scale
  else if constexpr (requires { p.mant; p.scale; }) {
    return static_cast<double>(p.mant) * std::pow(10.0, -static_cast<double>(p.scale));
  }
  // value = raw * 10^exponent
  else if constexpr (requires { p.raw; p.exponent; }) {
    return static_cast<double>(p.raw) * std::pow(10.0, static_cast<double>(p.exponent));
  }
  // Fallback: unknown layout
  else {
    return 0.0;
  }
}
// ---------- DIRECT field readers (per your SDK ToString dump) ----------
template <class M>
static inline auto get_bid_px_raw(const M& m) { return m.levels[0].bid_px; }

template <class M>
static inline auto get_ask_px_raw(const M& m) { return m.levels[0].ask_px; }

template <class M>
static inline QtyI get_bid_sz(const M& m) { return static_cast<QtyI>(m.levels[0].bid_sz); }

template <class M>
static inline QtyI get_ask_sz(const M& m) { return static_cast<QtyI>(m.levels[0].ask_sz); }

template <class T>
static inline auto get_px_raw(const T& r) { return r.price; }

template <class T>
static inline QtyI get_sz(const T& r) { return static_cast<QtyI>(r.size); }

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

// ---------- main loader (DbnFileStore::Replay + Record::GetIf) ----------
DayEvents load_day_from_dbn(const std::string& path, const std::string& schema_name) {
  DayEvents out;

  databento::DbnFileStore store{std::filesystem::path{path}};

  store.Replay([&](const databento::Record& rec) -> databento::KeepGoing {
    if (schema_name == "mbp-1") {
      if (const auto* m = rec.GetIf<databento::Mbp1Msg>()) {
        QuoteL1 q{};
        q.ts     = get_ts_ns(*m);
        q.bid_px = px_to_double(get_bid_px_raw(*m));  // PriceI is double now
        q.ask_px = px_to_double(get_ask_px_raw(*m));
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
