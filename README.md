# DBMS

A simple relational DB built during the Bradfield Sabbatical program.

## Objective
To learn how a relational DBMS system works from first principles

## Overview
- `/data` directory will contain the csv and heap files.
- `heap_file.c`, given a schema and a CSV file, it outputs their corresponding [heapfile](http://pages.cs.wisc.edu/~dbbook/openAccess/Minibase/spaceMgr/heap_file.html)
- `executor.c` contains all implemented nodes:
  - `scan`: it scans tuples from heap files one at a time.
  - `select`: given a predicate functions, it tests a tuple against it if the test returns true.
  - `project`: it filters out the fields not present in the result schema from a tuple and yields a new tuple
  that conforms to the result schema
  - `average`
- `serializer.c`, used to serialize char, int into binary and vice versa.

## Run
You will first need to download the [movie lens](https://grouplens.org/datasets/movielens/) data into the `/data` directory. You'll only need the movies.csv and ratings.csv. 

Once that's done you need to run the following command to convert the csv files to a heap file that the DB can use.
```
  gcc -o heap_file serializer.c heap_file.c -I.
  ./heap_file
```

After that's done you can run the query executor
```
 gcc -o executor serializer.c executor.c heap_file.c -I.
 ./executor
```