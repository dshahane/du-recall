# Ensure you install rockdb
# brew install rocksdb
# 1. Force the C++17 standard for C/C++ compilation.
# This is a more robust way to force the standard than CXXFLAGS alone.
export CFLAGS="-std=c++17"
export CXXFLAGS="-std=c++17"

# 2. Set the path to the RocksDB headers/libraries installed by Homebrew.
# This addresses the 'backupable_db.h' file not found error.
export CPATH="/opt/homebrew/include"
export LIBRARY_PATH="/opt/homebrew/lib"
export LDFLAGS="-L/opt/homebrew/lib -Wl,-rpath,/opt/homebrew/lib"
export ROCKSDB_HOME="/opt/homebrew"