add_library_unity(
        duckdb_sqlpgq_functions
        OBJECT
        sqlpgq_csr_creation.cpp
        sqlpgq_cheapest_path_length.cpp
        sqlpgq_shortest_path.cpp
        sqlpgq_reachability.cpp
        sqlpgq_iterativelength.cpp
        sqlpgq_iterativelength2.cpp
        sqlpgq_bidirectional_iterativelength.cpp
        sqlpgq_iterativelength_bidirectional.cpp
        sqlpgq_iterativelength_lane_reuse.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_sqlpgq_functions>
        PARENT_SCOPE)
