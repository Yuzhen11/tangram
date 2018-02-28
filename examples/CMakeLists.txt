include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

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

# SimpleExample
add_executable(SimpleExample simple_example.cpp)
target_link_libraries(SimpleExample xyz)
target_link_libraries(SimpleExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET SimpleExample PROPERTY CXX_STANDARD 11)
add_dependencies(SimpleExample ${external_project_dependencies})

# LoadExample
add_executable(LoadExample load_example.cpp)
target_link_libraries(LoadExample xyz)
target_link_libraries(LoadExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET LoadExample PROPERTY CXX_STANDARD 11)
add_dependencies(LoadExample ${external_project_dependencies})


# SchedulerMain
add_executable(SchedulerMain scheduler_main.cpp)
target_link_libraries(SchedulerMain xyz)
target_link_libraries(SchedulerMain ${HUSKY_EXTERNAL_LIB})
set_property(TARGET SchedulerMain PROPERTY CXX_STANDARD 11)
add_dependencies(SchedulerMain ${external_project_dependencies})
