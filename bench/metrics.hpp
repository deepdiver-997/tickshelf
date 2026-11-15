#pragma once
#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <map>
#include <sstream>
#include <string>

#include "record.hpp"

namespace tickshelf::bench {
inline std::uint64_t clock_ns() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count());
}

struct Histogram {
    std::uint64_t count = 0, sum_ns = 0, max_ns = 0;
    std::array<std::uint64_t, 64> bins{};
    void add(std::uint64_t ns) {
        ++count;
        sum_ns += ns;
        max_ns = std::max(max_ns, ns);
        ++bins[std::min<std::size_t>(63, std::bit_width(ns))];
    }
    std::uint64_t percentile(unsigned percent) const {
        if (!count) return 0;
        const auto rank = (count * percent + 99) / 100;
        std::uint64_t cumulative = 0;
        for (unsigned i = 0; i < 64; ++i) {
            cumulative += bins[i];
            if (cumulative >= rank) return i == 63 ? max_ns : std::min(max_ns, (1ULL << i) - 1);
        }
        return max_ns;
    }
};

struct Metrics {
    std::map<std::string, std::uint64_t> counters;
    std::map<std::string, Histogram> stages;
    void inc(const std::string& name, std::uint64_t n = 1) { counters[name] += n; }
    void time(const std::string& name, std::uint64_t ns) { stages[name].add(ns); }
    std::string json() const {
        std::ostringstream out;
        out << "{\"counters\":{";
        bool comma = false;
        for (const auto& [name, value] : counters) {
            if (comma) out << ',';
            comma = true;
            out << json_quote(name) << ':' << value;
        }
        out << "},\"stages\":{";
        comma = false;
        for (const auto& [name, h] : stages) {
            if (comma) out << ',';
            comma = true;
            out << json_quote(name) << ":{\"count\":" << h.count << ",\"sum_ns\":" << h.sum_ns
                << ",\"max_ns\":" << h.max_ns << ",\"p50_upper_ns\":" << h.percentile(50)
                << ",\"p95_upper_ns\":" << h.percentile(95) << ",\"p99_upper_ns\":" << h.percentile(99) << '}';
        }
        out << "}}";
        return out.str();
    }
};

class Timer {
public:
    Timer(Metrics& metrics, std::string name) : metrics_(metrics), name_(std::move(name)), start_(clock_ns()) {}
    ~Timer() { metrics_.time(name_, clock_ns() - start_); }
private:
    Metrics& metrics_;
    std::string name_;
    std::uint64_t start_;
};
}  // namespace tickshelf::bench
