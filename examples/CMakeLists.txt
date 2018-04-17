include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

add_subdirectory(pagerank)
add_subdirectory(tfidf)
add_subdirectory(lr)
add_subdirectory(kmeans)

# SchedulerExample
add_executable(SchedulerExample scheduler_example.cpp)
target_link_libraries(SchedulerExample xyz)
target_link_libraries(SchedulerExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET SchedulerExample PROPERTY CXX_STANDARD 11)
add_dependencies(SchedulerExample ${external_project_dependencies})

# WorkerExample
add_executable(WorkerExample worker_example.cpp)
target_link_libraries(WorkerExample xyz)
target_link_libraries(WorkerExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET WorkerExample PROPERTY CXX_STANDARD 11)
add_dependencies(WorkerExample ${external_project_dependencies})

# LoadExample
add_executable(LoadExample load_example.cpp)
target_link_libraries(LoadExample xyz)
target_link_libraries(LoadExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET LoadExample PROPERTY CXX_STANDARD 14)
add_dependencies(LoadExample ${external_project_dependencies})

# GraphMatching 
add_executable(GraphMatching graph_matching.cpp)
target_link_libraries(GraphMatching xyz)
target_link_libraries(GraphMatching ${HUSKY_EXTERNAL_LIB})
set_property(TARGET GraphMatching PROPERTY CXX_STANDARD 14)
add_dependencies(GraphMatching ${external_project_dependencies})

# SchedulerMain
add_executable(SchedulerMain scheduler_main.cpp)
target_link_libraries(SchedulerMain xyz)
target_link_libraries(SchedulerMain ${HUSKY_EXTERNAL_LIB})
set_property(TARGET SchedulerMain PROPERTY CXX_STANDARD 11)
add_dependencies(SchedulerMain ${external_project_dependencies})

# A
add_executable(A a.cpp)
target_link_libraries(A xyz)
target_link_libraries(A ${HUSKY_EXTERNAL_LIB})
set_property(TARGET A PROPERTY CXX_STANDARD 14)
add_dependencies(A ${external_project_dependencies})

# TestFetch
add_executable(TestFetch test_fetch.cpp)
target_link_libraries(TestFetch xyz)
target_link_libraries(TestFetch ${HUSKY_EXTERNAL_LIB})
set_property(TARGET TestFetch PROPERTY CXX_STANDARD 14)
add_dependencies(TestFetch ${external_project_dependencies})

# Crawler
add_executable(Crawler crawler.cpp)
target_link_libraries(Crawler xyz)
target_link_libraries(Crawler ${HUSKY_EXTERNAL_LIB})
set_property(TARGET Crawler PROPERTY CXX_STANDARD 14)
add_dependencies(Crawler ${external_project_dependencies})

# Nomad
add_executable(Nomad nomad2.cpp)
target_link_libraries(Nomad xyz)
target_link_libraries(Nomad ${HUSKY_EXTERNAL_LIB})
set_property(TARGET Nomad PROPERTY CXX_STANDARD 14)
add_dependencies(Nomad ${external_project_dependencies})

# TestLB
add_executable(TestLB test_lb.cpp)
target_link_libraries(TestLB xyz)
target_link_libraries(TestLB ${HUSKY_EXTERNAL_LIB})
set_property(TARGET TestLB PROPERTY CXX_STANDARD 14)

# TestCP
add_executable(TestCP test_cp.cpp)
target_link_libraries(TestCP xyz)
target_link_libraries(TestCP ${HUSKY_EXTERNAL_LIB})
set_property(TARGET TestCP PROPERTY CXX_STANDARD 14)
add_dependencies(TestCP ${external_project_dependencies})
