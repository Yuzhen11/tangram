include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB core-src-files
    engine.cpp
    tmp.cpp
    partition/partition_manager.cpp
    executor/thread_pool.cpp
    executor/executor.cpp
    plan/function_store.cpp
    map_output/map_output_storage.cpp
  )

add_library(core-objs OBJECT ${core-src-files})
set_property(TARGET core-objs PROPERTY CXX_STANDARD 11)
add_dependencies(core-objs ${external_project_dependencies})
