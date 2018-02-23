include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB core-src-files
    partition/partition_manager.cpp
    partition/fetcher.cpp
    partition/partition_tracker.cpp
    executor/thread_pool.cpp
    executor/executor.cpp
    plan/function_store.cpp
    map_output/map_output_storage.cpp
    scheduler/scheduler.cpp
  )

# TODO now we let engine and worker depends on HDFS 
# so that they won't be built for unit test.
if(LIBHDFS3_FOUND)
    file(GLOB core-src-hdfs-files 
        engine.cpp
        scheduler/worker.cpp)
    list(APPEND core-src-files ${core-src-hdfs-files})
endif(LIBHDFS3_FOUND)

add_library(core-objs OBJECT ${core-src-files})
set_property(TARGET core-objs PROPERTY CXX_STANDARD 11)
add_dependencies(core-objs ${external_project_dependencies})
