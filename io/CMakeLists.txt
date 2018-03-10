include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

file(GLOB io-src-files
	assigner.cpp
    block_reader_wrapper.cpp
    reader_wrapper.cpp
    writer_wrapper.cpp
    io_wrapper.cpp
	)

if(LIBHDFS3_FOUND)
    file(GLOB io-src-hdfs-files
        hdfs_browser.cpp
        hdfs_block_reader.cpp
        hdfs_reader.cpp
        hdfs_writer.cpp)
    list(APPEND io-src-files ${io-src-hdfs-files})
endif(LIBHDFS3_FOUND)

add_library(io-objs OBJECT ${io-src-files})
set_property(TARGET io-objs PROPERTY CXX_STANDARD 11)
add_dependencies(io-objs ${external_project_dependencies})

if(LIBHDFS3_FOUND)
    add_executable(HDFSAssignerMain hdfs_assigner_main.cpp)
    target_link_libraries(HDFSAssignerMain xyz)
    target_link_libraries(HDFSAssignerMain ${HUSKY_EXTERNAL_LIB})
    set_property(TARGET HDFSAssignerMain PROPERTY CXX_STANDARD 11)
    add_dependencies(HDFSAssignerMain ${external_project_dependencies})

    add_executable(HDFSBlockReaderMain hdfs_block_reader_main.cpp)
    target_link_libraries(HDFSBlockReaderMain xyz)
    target_link_libraries(HDFSBlockReaderMain ${HUSKY_EXTERNAL_LIB})
    set_property(TARGET HDFSBlockReaderMain PROPERTY CXX_STANDARD 11)
    add_dependencies(HDFSBlockReaderMain ${external_project_dependencies})

    add_executable(HDFSReaderMain hdfs_reader_main.cpp)
    target_link_libraries(HDFSReaderMain xyz)
    target_link_libraries(HDFSReaderMain ${HUSKY_EXTERNAL_LIB})
    set_property(TARGET HDFSReaderMain PROPERTY CXX_STANDARD 11)
    add_dependencies(HDFSReaderMain ${external_project_dependencies})

    add_executable(HDFSWriterMain hdfs_writer_main.cpp)
    target_link_libraries(HDFSWriterMain xyz)
    target_link_libraries(HDFSWriterMain ${HUSKY_EXTERNAL_LIB})
    set_property(TARGET HDFSWriterMain PROPERTY CXX_STANDARD 11)
    add_dependencies(HDFSWriterMain ${external_project_dependencies})
endif(LIBHDFS3_FOUND)
