<img src="logo/bustub-whiteborder.svg" alt="BusTub Logo" height="200">

-----------------

[![Build Status](https://travis-ci.org/cmu-db/bustub.svg?branch=master)](https://travis-ci.org/cmu-db/bustub)
[![CircleCI](https://circleci.com/gh/cmu-db/bustub/tree/master.svg?style=svg)](https://circleci.com/gh/cmu-db/bustub/tree/master)

BusTub is a relational database management system built at [Carnegie Mellon University](https://db.cs.cmu.edu) for the [Introduction to Database Systems](https://15445.courses.cs.cmu.edu) (15-445/645) course. This system was developed for educational purposes and should not be used in production environments.

**WARNING: IF YOU ARE A STUDENT IN THE CLASS, DO NOT DIRECTLY FORK THIS REPO. DO NOT PUSH PROJECT SOLUTIONS PUBLICLY. THIS IS AN ACADEMIC INTEGRITY VIOLATION AND CAN LEAD TO GETTING YOUR DEGREE REVOKED, EVEN AFTER YOU GRADUATE.**

## Preface
TODO: the purpose of this project

## Project 0
TODO: line coverage report

## Project 1
### Test Coverage
![](https://github.com/MeteorYee/still-working/blob/dev/master/images/project1_test_coverage.png)

As the image shows above, the test cases achieve nearly 100% line coverage for the project. There is only one line not covered which might be tested in the future when I can come up with a tricky test case against it.

## Project 2
### Test Coverage
![](https://github.com/MeteorYee/still-working/blob/dev/master/images/project2_page_test_lc.png)

*hash_table_page_test* has covered all the normal lines in *hash_table_directory_page.cpp* and *hash_table_bucket_page.cpp*. Because there are some assertion codes (the program shall not run into these codes, otherwise it will abort), the image above shows *hash_table_directory_page.cpp* is not 100% covered.

![](https://github.com/MeteorYee/still-working/blob/dev/master/images/project2_table_test_lc.png)

*hash_table_test* doesn't fully cover all the lines in *extendible_hash_table.cpp* due to 2 extreme cases: (1) split-insert failure; (2) merge failure caused by a false-positive empty bucket. Test cases against those two scenarios may be added in the future.

## Project 3
# Test Coverage

![](https://github.com/MeteorYee/still-working/blob/dev/master/images/project3-test-coverage.png)

All the basic funtionality have been tested, whereas some of the exceptional cases are not covered yet. Those exceptions will be taken care of in the future in accordance with the transaction relevant stuff.

## Test Coverage Settings
```
# add the two lines below into the cmake file
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")

# MAKE SURE you run the test once !!!

# then
lcov --capture --directory . --output-file bustub-test.info --test-name bustub-test -b .

# finally generate the html directory
genhtml bustub-test.info --output-directory bustub-lcov.dir
```