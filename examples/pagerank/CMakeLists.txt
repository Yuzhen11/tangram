include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# PageRank
add_executable(PageRank pagerank.cpp)
target_link_libraries(PageRank xyz)
target_link_libraries(PageRank ${HUSKY_EXTERNAL_LIB})
set_property(TARGET PageRank PROPERTY CXX_STANDARD 14)
add_dependencies(PageRank ${external_project_dependencies})

# PageRankWith
add_executable(PageRankWith pagerank_with.cpp)
target_link_libraries(PageRankWith xyz)
target_link_libraries(PageRankWith ${HUSKY_EXTERNAL_LIB})
set_property(TARGET PageRankWith PROPERTY CXX_STANDARD 14)
add_dependencies(PageRankWith ${external_project_dependencies})

# PageRankConverge
add_executable(PageRankConverge pagerank-converge.cpp)
target_link_libraries(PageRankConverge xyz)
target_link_libraries(PageRankConverge ${HUSKY_EXTERNAL_LIB})
set_property(TARGET PageRankConverge PROPERTY CXX_STANDARD 14)
add_dependencies(PageRankConverge ${external_project_dependencies})

# ComparePR
add_executable(ComparePR compare_pr.cpp)
target_link_libraries(ComparePR xyz)
target_link_libraries(ComparePR ${HUSKY_EXTERNAL_LIB})
set_property(TARGET ComparePR PROPERTY CXX_STANDARD 14)
add_dependencies(ComparePR ${external_project_dependencies})
