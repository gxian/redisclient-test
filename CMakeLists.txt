cmake_minimum_required(VERSION 2.8)
project(RedisClientTest)

set(BOOST_COMPONENTS system thread date_time chrono atomic)
set(CMAKE_CXX_FLAGS   "${CMAKE_CXX_FLAGS} -std=c++11")
add_definitions(-DUSE_BOOST_ASIO=1)

find_package(Boost 1.55.0 COMPONENTS ${BOOST_COMPONENTS} REQUIRED)
include_directories(SYSTEM ${Boost_INCLUDE_DIR})

if(MSVC)
    add_definitions(-D_SCL_SECURE_NO_WARNINGS -wd4996 -wd4819) 
    add_definitions(-DBOOST_ALL_DYN_LINK)
    add_definitions(-D_WIN32_WINNT=0x0501)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /SAFESEH:NO")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /SAFESEH:NO")
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} /SAFESEH:NO")
else()
    if (CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 4.9)
        set(CMAKE_C_COMPILER "gcc")
        set(CMAKE_CXX_COMPILER "g++")
    else()
        set(CMAKE_C_COMPILER "clang")
        set(CMAKE_CXX_COMPILER "clang++")
    endif()
    if(WITHOUT_ASAN)
        set(CMAKE_CXX_FLAGS   "${CMAKE_CXX_FLAGS} -Wall -g -O2")
    else()
        set(CMAKE_CXX_FLAGS   "${CMAKE_CXX_FLAGS} -Wall -g -O2 -fsanitize=address -fno-omit-frame-pointer")
    endif(WITHOUT_ASAN)
endif(MSVC)


if(APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
else()
	if(UNIX)
    		set(CMAKE_CXX_FLAGS   "${CMAKE_CXX_FLAGS} -pthread")
	endif()
endif()

include_directories("${PROJECT_SOURCE_DIR}")
include_directories("${PROJECT_SOURCE_DIR}/dep")
file(GLOB SRC *.cpp)

# service bianry
add_executable(redisclient_test 
	${SRC}
)

target_link_libraries(redisclient_test ${Boost_LIBRARIES})
set_target_properties(redisclient_test
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/bin"
)

