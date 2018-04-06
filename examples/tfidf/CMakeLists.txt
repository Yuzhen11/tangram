include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# TFIDF
add_executable(TFIDF tfidf.cpp)
target_link_libraries(TFIDF xyz)
target_link_libraries(TFIDF ${HUSKY_EXTERNAL_LIB})
set_property(TARGET TFIDF PROPERTY CXX_STANDARD 14)
add_dependencies(TFIDF ${external_project_dependencies})
add_dependencies(TFIDF ${external_project_dependencies})


# TFIDF2
add_executable(TFIDF2 tfidf_v2.cpp)
target_link_libraries(TFIDF2 xyz)
target_link_libraries(TFIDF2 ${HUSKY_EXTERNAL_LIB})
set_property(TARGET TFIDF2 PROPERTY CXX_STANDARD 14)
add_dependencies(TFIDF2 ${external_project_dependencies})
add_dependencies(TFIDF2 ${external_project_dependencies})
