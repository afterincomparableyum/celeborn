# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Finds CRoaring library.
#
# This module defines:
# CROARING_FOUND
# CROARING_INCLUDE_DIR
# CROARING_LIBRARY
#

find_path(CROARING_INCLUDE_DIR NAMES roaring/roaring.hh)

find_library(CROARING_LIBRARY_DEBUG NAMES roaringd)
find_library(CROARING_LIBRARY_RELEASE NAMES roaring)

include(SelectLibraryConfigurations)
SELECT_LIBRARY_CONFIGURATIONS(CROARING)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    CRoaring DEFAULT_MSG
    CROARING_LIBRARY CROARING_INCLUDE_DIR
)

if (CROARING_FOUND)
    message(STATUS "Found CRoaring: ${CROARING_LIBRARY}")
endif()

mark_as_advanced(CROARING_INCLUDE_DIR CROARING_LIBRARY)
