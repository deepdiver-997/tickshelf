#pragma once
#include "store.hpp"

namespace tickshelf::bench {
// Called by open_store after common option validation. Disabled builds fail explicitly.
std::unique_ptr<Store> open_rocks_record_store(const StoreOptions& options, Metrics& metrics);
}
