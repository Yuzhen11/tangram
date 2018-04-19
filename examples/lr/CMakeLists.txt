include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# DenseLRExample
add_executable(DenseLRExample dense_lr.cpp)
target_link_libraries(DenseLRExample xyz)
target_link_libraries(DenseLRExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET DenseLRExample PROPERTY CXX_STANDARD 14)
add_dependencies(DenseLRExample ${external_project_dependencies})
add_dependencies(DenseLRExample ${external_project_dependencies})

# DenseLRExample2
add_executable(DenseLRExample2 dense_lr_2.cpp)
target_link_libraries(DenseLRExample2 xyz)
target_link_libraries(DenseLRExample2 ${HUSKY_EXTERNAL_LIB})
set_property(TARGET DenseLRExample2 PROPERTY CXX_STANDARD 14)
add_dependencies(DenseLRExample2 ${external_project_dependencies})
add_dependencies(DenseLRExample2 ${external_project_dependencies})

# SparseLRExample
add_executable(SparseLRExample sparse_lr.cpp)
target_link_libraries(SparseLRExample xyz)
target_link_libraries(SparseLRExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET SparseLRExample PROPERTY CXX_STANDARD 14)
add_dependencies(SparseLRExample ${external_project_dependencies})
add_dependencies(SparseLRExample ${external_project_dependencies})

# DenseLRRowExample
add_executable(DenseLRRowExample dense_lr_row.cpp)
target_link_libraries(DenseLRRowExample xyz)
target_link_libraries(DenseLRRowExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET DenseLRRowExample PROPERTY CXX_STANDARD 14)
add_dependencies(DenseLRRowExample ${external_project_dependencies})
add_dependencies(DenseLRRowExample ${external_project_dependencies})
