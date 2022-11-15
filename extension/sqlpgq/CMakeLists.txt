cmake_minimum_required(VERSION 2.8.12)

project(SQLPGQExtension)

include_directories(include)
add_subdirectory(sqlpgq_functions)

set(SQLPGQ_EXTENSION_FILES
        sqlpgq-extension.cpp
        sqlpgq_common.cpp
        sqlpgq_functions/sqlpgq_cheapest_path_length.cpp
        sqlpgq_functions/sqlpgq_csr_creation.cpp
        sqlpgq_functions/sqlpgq_reachability.cpp
        sqlpgq_functions/sqlpgq_iterativelength.cpp
        sqlpgq_functions/sqlpgq_shortest_path.cpp
        sqlpgq_functions/sqlpgq_iterativelength2.cpp
        sqlpgq_functions/sqlpgq_bidirectional_iterativelength.cpp
        sqlpgq_functions/sqlpgq_iterativelength_bidirectional.cpp
        sqlpgq_common.cpp)

add_library(sqlpgq_extension STATIC ${SQLPGQ_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(sqlpgq ${PARAMETERS} ${SQLPGQ_EXTENSION_FILES})

install(
        TARGETS sqlpgq_extension
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
