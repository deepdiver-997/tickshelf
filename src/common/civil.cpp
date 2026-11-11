#include "civil.hpp"

namespace tickshelf {

void civil_from_days(std::int64_t z, int& y, unsigned& m, unsigned& d) {
    z += 719468;
    const std::int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);                // [0, 146096]
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;  // [0, 399]
    const std::int64_t yy = static_cast<std::int64_t>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);  // [0, 365]
    const unsigned mp = (5 * doy + 2) / 153;                       // [0, 11]
    d = doy - (153 * mp + 2) / 5 + 1;                              // [1, 31]
    m = mp < 10 ? mp + 3 : mp - 9;                                 // [1, 12]
    y = static_cast<int>(yy + (m <= 2));
}

std::int64_t days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2;
    const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);           // [0, 399]
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;  // [0, 365]
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;           // [0, 146096]
    return era * 146097 + static_cast<std::int64_t>(doe) - 719468;
}

std::uint32_t yyyymmdd_from_ms(std::uint64_t ts_exchange_ms) {
    int y;
    unsigned m, d;
    civil_from_days(static_cast<std::int64_t>(ts_exchange_ms / kMsPerDay), y, m, d);
    return static_cast<std::uint32_t>(y) * 10000u + m * 100u + d;
}

}  // namespace tickshelf
