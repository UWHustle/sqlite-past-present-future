# Performance evaluation of SQLite3

## Requirements

* CMake >= 3.16

## How to run the benchmarks

First, ensure you have CMake >= 3.16 installed.

Then, run CMake to configure the project:
```
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
```

Build the executables:
```
cmake --build .
```
Executables for *SSB*, *TATP*, and *Blob* will be placed in their respective directories.

Modify the permissions of the scripts:
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
