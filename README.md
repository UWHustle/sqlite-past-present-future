# Performance evaluation of SQLite3

## How to run the benchmarks

First, build the executables:
```
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```
Executables for *SSB*, *TATP*, and *Blob* will be placed in their respective directories.

Then, modify the permissions of the scripts:
```
chmod u+x ssb/ssb.sh
chmod u+x tatp/tatp.sh
chmod u+x blob/blob.sh
chmod u+x all.sh
```

Run a specific benchmark by moving to its directory and then executing the script. For example:
```
cd ssb
./ssb.sh
```

Run all benchmarks by executing the *all* script:
```
./all.sh
```
