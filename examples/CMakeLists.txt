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

# MailboxExample
add_executable(MailboxExample mailbox_example.cpp)
target_link_libraries(MailboxExample xyz)
target_link_libraries(MailboxExample ${HUSKY_EXTERNAL_LIB})
set_property(TARGET MailboxExample PROPERTY CXX_STANDARD 11)
add_dependencies(MailboxExample ${external_project_dependencies})