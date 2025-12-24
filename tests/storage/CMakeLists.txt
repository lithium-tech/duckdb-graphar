log_stage("storage")
add_library_unity(test_storage OBJECT test_graphar_storage.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_storage>
    PARENT_SCOPE)
