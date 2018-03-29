include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB core-src-files
    partition/partition_manager.cpp
    cache/fetcher.cpp
    partition/partition_tracker.cpp
    executor/thread_pool.cpp
    executor/executor.cpp
    plan/function_store.cpp
    map_output/map_output_storage.cpp
    scheduler/control.cpp
    scheduler/scheduler_elem.cpp
    scheduler/worker.cpp
    scheduler/dag_runner.cpp
    scheduler/scheduler.cpp
    scheduler/block_manager.cpp
    scheduler/control_manager.cpp
    scheduler/write_manager.cpp
    scheduler/distribute_manager.cpp
    scheduler/collection_manager.cpp
    scheduler/checkpoint_manager.cpp
    scheduler/recover_manager.cpp
    scheduler/checkpoint_loader.cpp
    scheduler/collection_status.cpp
    plan/context.cpp
    plan/spec_wrapper.cpp
    plan/dag.cpp
    worker/controller.cpp
    worker/plan_controller.cpp
    worker/delayed_combiner.cpp
  )

# TODO now we let engine and worker depends on HDFS
# so that they won't be built for unit test.
if(LIBHDFS3_FOUND)
    file(GLOB core-src-hdfs-files
        engine.cpp
        plan/runner.cpp
        )
    list(APPEND core-src-files ${core-src-hdfs-files})
endif(LIBHDFS3_FOUND)

add_library(core-objs OBJECT ${core-src-files})
set_property(TARGET core-objs PROPERTY CXX_STANDARD 14)
add_dependencies(core-objs ${external_project_dependencies})
