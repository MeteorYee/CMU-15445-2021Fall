## Project 1
`page.h` revised: page header lock added

### Test Coverage
```
cd <PROJECT_DIR>/build/src/CMakeFiles/bustub_shared.dir/buffer

# lru_replacer.cpp
gcov lru_replacer.gcno
lcov --capture --directory . --output-file lru_test.info
genhtml lru_test.info --output-directory lru_test.dir

# buffer_pool_manager_instance.cpp
gcov buffer_pool_manager_instance.gcno
lcov --capture --directory . --output-file buffer_test.info
genhtml buffer_test.info --output-directory buffer_test.dir
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