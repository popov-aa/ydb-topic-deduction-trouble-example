cmake_minimum_required(VERSION 3.12...3.31)
project(
  ydb-topic-deduction-trouble-example
  VERSION 0.0.1
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_LINKER_TYPE LLD)
set(BROTLI_BUNDLED_MODE OFF)

# Dependencies

set(CPM_DOWNLOAD_VERSION 0.40.8)
include(./cmake/get_cpm.cmake)
if(NOT COMMAND CPMAddPackage)
  message(FATAL_ERROR "Failed to find CPM to download a package.")
endif()
if(CMAKE_VERSION VERSION_LESS "3.25.0")
  message(
    WARNING
      "Please update to cmake 3.25+ to remove warnings from third-party libs")
endif()
# If A uses find_package(B), and we install A and B using CPM, then: 1. make
# sure to call write_package_stub in SetupB 2. make sure to call SetupB at the
# beginning of SetupA
function(write_package_stub PACKAGE_NAME)
  file(WRITE "${CMAKE_BINARY_DIR}/package_stubs/${PACKAGE_NAME}Config.cmake" )
endfunction()

# jwt

cpmaddpackage(
  NAME
  jwt-cpp
  VERSION
  0.7.0
  GITHUB_REPOSITORY
  Thalhammer/jwt-cpp
  OPTIONS
  "JWT_BUILD_EXAMPLES OFF")
write_package_stub(jwt-cpp)

# Brotli

cpmaddpackage(NAME Brotli VERSION 1.1.0 GITHUB_REPOSITORY google/brotli)

if(NOT TARGET Brotli::dec)
  add_library(Brotli::dec ALIAS brotlidec)
endif()
if(NOT TARGET Brotli::enc)
  add_library(Brotli::enc ALIAS brotlienc)
endif()

# base64

cpmaddpackage(NAME base64 VERSION 0.5.2 GITHUB_REPOSITORY aklomp/base64)
write_package_stub(base64)
add_library(aklomp::base64 ALIAS base64)

#

# CPMAddPackage( NAME api-common-protos VERSION 1.50.0 GITHUB_REPOSITORY
# googleapis/api-common-protos GIT_TAG 1.50.0 # DOWNLOAD_ONLY YES )

# ydb-cpp-sdk

cpmaddpackage(
  NAME
  ydb-cpp-sdk
  GIT_TAG
  e9b5b8f
  GITHUB_REPOSITORY
  ydb-platform/ydb-cpp-sdk
  OPTIONS
  "YDB_SDK_EXAMPLES OFF"
  "Brotli_VERSION ${Brotli_VERSION}"
  # "RAPIDJSON_INCLUDE_DIRS ${RAPIDJSON_INCLUDE_DIRS}"
  # "YDB_SDK_GOOGLE_COMMON_PROTOS_TARGET ${api-common-proto_LIBRARY}"
)

find_package(Boost REQUIRED COMPONENTS program_options)

add_executable(
  ${PROJECT_NAME} src/main.cpp)

target_link_libraries(
  ${PROJECT_NAME}
  PUBLIC Boost::program_options YDB-CPP-SDK::Query YDB-CPP-SDK::Params
         YDB-CPP-SDK::Driver YDB-CPP-SDK::Scheme YDB-CPP-SDK::Topic)

target_include_directories(
  ${PROJECT_NAME}
  PRIVATE src
          ${CMAKE_CURRENT_BINARY_DIR}
          ${ydb-cpp-sdk_INCLUDE_DIRS}
          ${ydb-cpp-sdk_SOURCE_DIR}/include
          ${ydb-cpp-sdk_SOURCE_DIR}
          ${ydb-cpp-sdk_BINARY_DIR} )

# Install

set(CPACK_GENERATOR "TGZ")
include(CPack)

install(
  TARGETS ${PROJECT_NAME}
  DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT ${PROJECT_NAME})
