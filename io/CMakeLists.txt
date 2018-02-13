include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNALdd_INCLUDE})

#if(LIBHDFS3_FOUND)
file(GLOB io-src-files hdfs_assigner.cpp hdfs_loader.cpp)
#endif(LIBHDFS3_FOUND)

add_library(io-objs OBJECT ${io-src-files})
set_property(TARGET io-objs PROPERTY CXX_STANDARD 11)
add_dependencies(io-objs ${external_project_dependencies})
