#include <gtest/gtest.h>
#include "storage/iterator.h"
TEST(StoragePlaceholder, EntryDefaults) { raftkv::Entry e; EXPECT_FALSE(e.tombstone); }
