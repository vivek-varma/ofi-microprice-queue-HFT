#pragma once
#include <cstdint>

using TsNanos = std::int64_t;
using PriceI  = double;
using QtyI    = std::int32_t;

struct QuoteL1 {
  TsNanos ts{0};
  PriceI  bid_px{0}, ask_px{0};
  QtyI    bid_sz{0}, ask_sz{0};
};

enum class Aggressor : std::uint8_t { Unknown=0, Buy=1, Sell=2 };
struct Trade {
  TsNanos ts{0};
  PriceI  px{0};
  QtyI    sz{0};
  Aggressor side{Aggressor::Unknown};
};

enum class EvType : std::uint8_t { Quote=0, Trade=1 };
struct Event {
  EvType   type{EvType::Quote};
  TsNanos  ts{0};
  QuoteL1  q{};
  Trade    t{};
};
