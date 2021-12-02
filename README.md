## Project 0
need line coverage report? and some descriptions...

## Project 1
`page.h` revised: page header lock added

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