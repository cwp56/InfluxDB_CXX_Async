project(Influxdb_Async)

cmake_minimum_required(VERSION 3.16)
set(CMAKE_CXX_STANDARD 17)

include_directories(.)
include_directories(include)
include_directories(src)
include_directories(test)

find_package(Boost COMPONENTS system filesystem REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})

add_executable(Influxdb_Async
        main.cpp
        ThreadPool.h
        include/InfluxDB.h
        include/InfluxDBFactory.h
        include/Point.h
        include/Transport.h
        src/HTTP.cxx
        src/HTTP.h
        src/InfluxDB.cxx
        src/InfluxDBException.h
        src/InfluxDBFactory.cxx
        src/Point.cxx
        src/UDP.cxx
        src/UDP.h
        src/UnixSocket.cxx
        src/UnixSocket.h
        src/UriParser.h ThreadPool.h)
#add_executable(Influxdb_Async main.cpp)

target_link_libraries(Influxdb_Async curl)
target_link_libraries(Influxdb_Async ${Boost_LIBRARIES})

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

