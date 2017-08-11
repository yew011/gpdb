/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#ifndef GPDB_PXFFRAGMENT_H
#define GPDB_PXFFRAGMENT_H

#include "pxfuriparser.h"
#include "pxfutils.h"
#include "libchurl.h"
#include "pxfheaders.h"

/*
 * One debug level for all log messages from the data allocation algorithm
 */
#define FRAGDEBUG DEBUG2

#define REST_HEADER_JSON_RESPONSE "Accept: application/json"

typedef struct sPxfServer
{
    char *host;
    int port;
} PxfServer;

/*
 * A DataFragment instance contains the fragment data necessary
 * for the allocation algorithm
 */
typedef struct sDataFragment
{
    int   index; /* index per source name */
    char *source_name;
    List *replicas;
    char *fragment_md; /* fragment meta data (start, end, length, etc.) */
    char *user_data;
    char *profile;
} DataFragment;

/*
 * Represents a fragment location replica
 */
typedef struct sFragmentHost
{
    char *ip;
    int   rest_port;
} FragmentHost;

extern void set_fragments(GPHDUri* uri);
extern List* free_fragment_list(List *fragments);

#endif //GPDB_PXFFRAGMENT_H
