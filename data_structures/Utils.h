//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
//

#ifndef HARPC_UTILS_H
#define HARPC_UTILS_H

#include "Table.h"

namespace harp {
    namespace ds {
        namespace util {
            template<class TYPE>
            void deleteTable(harp::ds::Table<TYPE> *table, bool clearPartitions);

            template<class TYPE>
            void deleteTables(harp::ds::Table<TYPE> **tables, bool clearPartitions, int arrSize);
        }
    }
}
#endif //HARPC_UTILS_H