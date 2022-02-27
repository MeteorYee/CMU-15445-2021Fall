**WARNING: IF YOU ARE A STUDENT IN THE CLASS, DO NOT DIRECTLY FORK THIS REPO. DO NOT PUSH PROJECT SOLUTIONS PUBLICLY. THIS IS AN ACADEMIC INTEGRITY VIOLATION AND CAN LEAD TO GETTING YOUR DEGREE REVOKED, EVEN AFTER YOU GRADUATE.**

## Project 0
TODO: line coverage report

## Project 1
### Test Coverage
![](https://github.com/MeteorYee/still-working/blob/dev/master/images/project1_test_coverage.png)

As the image shows above, the test cases achieve nearly 100% line coverage for the project. There is only one line not covered which might be tested in the future when I can come up with a tricky test case against it.

### Test Coverage Calculation
```
cd <PROJECT_DIR>/build/src/CMakeFiles/bustub_shared.dir/buffer

# lru_replacer.cpp
gcov lru_replacer.gcno
# buffer_pool_manager_instance.cpp
gcov buffer_pool_manager_instance.gcno
# parallel_buffer_pool_manager.cpp
gcov parallel_buffer_pool_manager.gcno

lcov --capture --directory . --output-file project1_test.info
genhtml project1_test.info --output-directory project1_test.dir
```

## Project 2
### Test Coverage
![](https://github.com/MeteorYee/still-working/blob/dev/master/images/project2_page_test_lc.png)

*hash_table_page_test* has covered all the normal lines in *hash_table_directory_page.cpp* and *hash_table_bucket_page.cpp*. Because there are some assertion codes (the program shall not run into these codes, otherwise it will abort), the image above shows *hash_table_directory_page.cpp* is not 100% covered.

![](https://github.com/MeteorYee/still-working/blob/dev/master/images/project2_table_test_lc.png)

*hash_table_test* doesn't fully cover all the lines in *extendible_hash_table.cpp* due to 2 extreme cases: (1) split-insert failure; (2) merge failure caused by a false-positive empty bucket. Test cases against those two scenarios may be added in the future.

### Test Coverage Calculation
```
cd <PROJECT_DIR>/build/src/CMakeFiles/bustub_shared.dir/storage/page
# hash_table_directory_page.cpp
gcov hash_table_directory_page.gcno
# hash_table_bucket_page.cpp
gcov hash_table_bucket_page.gcno
lcov --capture --directory . --output-file hash_table_page_test.info
genhtml hash_table_page_test.info --output-directory hash_table_page_test.dir

cd <PROJECT_DIR>/build/src/CMakeFiles/bustub_shared.dir/container/hash
# extendible_hash_table.cpp
gcov extendible_hash_table.gcno
lcov --capture --directory . --output-file hash_table_test.info
genhtml hash_table_test.info --output-directory hash_table_test.dir
```

## Project 3
# Test Coverage

![](https://github.com/MeteorYee/still-working/blob/dev/master/images/project3-test-coverage.png)

All the basic funtionality have been tested, whereas some of the exceptional cases are not covered yet. Those exceptions will be taken care of in the future in accordance with the transaction relevant stuff.

### Test Coverage Calculation
```
cd <PROJECT_DIR>/build/src/CMakeFiles/bustub_shared.dir/execution
# *_executor.cpp
gcov *_executor.gcno
lcov --capture --directory . --output-file executor_test.info
genhtml executor_test.info --output-directory executor_test.dir
```

## Test Coverage Settings
```
# add the two lines below into the cmake file
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
set(CMAKE_CXX_OUTPUT_EXTENSION_REPLACE 1)

# MAKE SURE you run the test once !!!

# find the .gcno file, should be under the path: "<PROJECT_PATH>/build/src/CMakeFiles/bustub_shared.dir/<OBJECT_FILE_PATH>"
gcov FILENAME.gcno

# then
lcov --capture --directory . --output-file <OBJECT_FILE>.info

# finally generate the html directory
genhtml <OBJECT_FILE>.info --output-directory <OBJECT_FOLDER_NAME>
```