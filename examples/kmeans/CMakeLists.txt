include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# KmeansExample
add_executable(KmeansExample kmeans.cpp)
target_link_libraries(KmeansExample xyz)
target_link_libraries(KmeansExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET KmeansExample PROPERTY CXX_STANDARD 14)
add_dependencies(KmeansExample ${external_project_dependencies})
add_dependencies(KmeansExample ${external_project_dependencies})

# KmeansRowExample
add_executable(KmeansRowExample kmeans_row.cpp)
target_link_libraries(KmeansRowExample xyz)
target_link_libraries(KmeansRowExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET KmeansRowExample PROPERTY CXX_STANDARD 14)
add_dependencies(KmeansRowExample ${external_project_dependencies})
add_dependencies(KmeansRowExample ${external_project_dependencies})
