**WARNING: IF YOU ARE A STUDENT IN THE CLASS, DO NOT DIRECTLY FORK THIS REPO. DO NOT PUSH PROJECT SOLUTIONS PUBLICLY. THIS IS AN ACADEMIC INTEGRITY VIOLATION AND CAN LEAD TO GETTING YOUR DEGREE REVOKED, EVEN AFTER YOU GRADUATE.**

## Project 0
TODO: line coverage report

## Project 1
### Test Coverage
![](https://github.com/MeteorYee/still-working/blob/dev/master/images/project1_test_coverage.png)

As the image shows above, the test cases achieve nearly 100% line coverage for the project. There is only one line not covered which might be tested in the future when I come up with a tricky test case against it.

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

# Project 2
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