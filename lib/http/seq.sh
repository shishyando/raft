#!/bin/bash

port=$1

echo "localhost:$port read key1" &&
python3.9 ./req.py $port read key1 &&
echo "------------------------" &&
echo "$port create key1 value1" &&
python3.9 ./req.py $port create key1 value1 &&
echo "------------------------" &&
echo "$port read key1" &&
python3.9 ./req.py $port read key1 &&
echo "------------------------" &&
echo "$port update key1 upd1" &&
python3.9 ./req.py $port update key1 upd1 &&
echo "------------------------" &&
echo "$port read key1" &&
python3.9 ./req.py $port read key1 &&
echo "------------------------" &&
echo "$port cas key1 val1 val2 key1" &&
python3.9 ./req.py $port cas key1 newValue expectedValue &&
echo "------------------------" &&
echo "$port cas key1 upd1 cased" &&
python3.9 ./req.py $port cas key1 cased upd1 &&
echo "------------------------" &&
echo "Done."
