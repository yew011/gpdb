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

#include <json-c/json.h>
#include "pxffragment.h"

static void rest_request(GPHDUri *hadoop_uri, ClientContext* client_context, char *rest_msg);
static List* parse_get_fragments_response(List *fragments, StringInfo rest_buf);
char* concat(int num_args, ...);

/*
 * get_data_fragment_list
 *
 * 1. Request a list of fragments from the PXF Fragmenter class
 * that was specified in the pxf URL.
 *
 * 2. Process the returned list and create a list of DataFragment with it.
 */
List*
get_data_fragment_list(GPHDUri *hadoop_uri,
                       ClientContext *client_context)
{
    List *data_fragments = NIL;
    char *restMsg = concat(2, "http://%s:%s/%s/%s/Fragmenter/getFragments?path=", hadoop_uri->data);

    rest_request(hadoop_uri, client_context, restMsg);

    /* parse the JSON response and form a fragments list to return */
    data_fragments = parse_get_fragments_response(data_fragments, &(client_context->the_rest_buf));

    return data_fragments;
}


/*
 * Wrap the REST call with a retry for the HA HDFS scenario
 */
static void
rest_request(GPHDUri *hadoop_uri, ClientContext* client_context, char *rest_msg)
{
    Assert(hadoop_uri->host != NULL && hadoop_uri->port != NULL);

    /* construct the request */
    call_rest(hadoop_uri, client_context, rest_msg);
}

/*
 * parse the response of the PXF Fragments call. An example:
 *
 * Request:
 * 		curl --header "X-GP-FRAGMENTER: HdfsDataFragmenter" "http://goldsa1mac.local:50070/pxf/v2/Fragmenter/getFragments?path=demo" (demo is a directory)
 *
 * Response (left as a single line purposefully):
 * {"PXFFragments":[{"index":0,"userData":null,"sourceName":"demo/text2.csv","metadata":"rO0ABXcQAAAAAAAAAAAAAAAAAAAABXVyABNbTGphdmEubGFuZy5TdHJpbmc7rdJW5+kde0cCAAB4cAAAAAN0ABxhZXZjZWZlcm5hczdtYnAuY29ycC5lbWMuY29tdAAcYWV2Y2VmZXJuYXM3bWJwLmNvcnAuZW1jLmNvbXQAHGFldmNlZmVybmFzN21icC5jb3JwLmVtYy5jb20=","replicas":["10.207.4.23","10.207.4.23","10.207.4.23"]},{"index":0,"userData":null,"sourceName":"demo/text_csv.csv","metadata":"rO0ABXcQAAAAAAAAAAAAAAAAAAAABnVyABNbTGphdmEubGFuZy5TdHJpbmc7rdJW5+kde0cCAAB4cAAAAAN0ABxhZXZjZWZlcm5hczdtYnAuY29ycC5lbWMuY29tdAAcYWV2Y2VmZXJuYXM3bWJwLmNvcnAuZW1jLmNvbXQAHGFldmNlZmVybmFzN21icC5jb3JwLmVtYy5jb20=","replicas":["10.207.4.23","10.207.4.23","10.207.4.23"]}]}
 */
static List*
parse_get_fragments_response(List *fragments, StringInfo rest_buf)
{
    struct json_object	*whole	= json_tokener_parse(rest_buf->data);
    if ((whole == NULL) || is_error(whole))
    {
        elog(ERROR, "Failed to parse fragments list from PXF");
    }
    struct json_object	*head;
    json_object_object_get_ex(whole, "PXFFragments", &head);
    List* 				ret_frags = fragments;
    int 				length	= json_object_array_length(head);

    /* obtain split information from the block */
    for (int i = 0; i < length; i++)
    {
        struct json_object *js_fragment = json_object_array_get_idx(head, i);
        DataFragment* fragment = (DataFragment*)palloc0(sizeof(DataFragment));

        /* 0. source name */
        struct json_object *block_data;
        if(json_object_object_get_ex(js_fragment, "sourceName", &block_data))
            fragment->source_name = pstrdup(json_object_get_string(block_data));

        /* 1. fragment index, incremented per source name */
        struct json_object *index;
        if(json_object_object_get_ex(js_fragment, "index", &index))
            fragment->index = json_object_get_int(index);

        /* 2. replicas - list of all machines that contain this fragment */
        int num_replicas = 0;
        struct json_object *js_fragment_replicas;
        if(json_object_object_get_ex(js_fragment, "replicas", &js_fragment_replicas))
            num_replicas = json_object_array_length(js_fragment_replicas);

        for (int j = 0; j < num_replicas; j++)
        {
            FragmentHost* fhost = (FragmentHost*)palloc(sizeof(FragmentHost));
            struct json_object *host = json_object_array_get_idx(js_fragment_replicas, j);

            fhost->ip = pstrdup(json_object_get_string(host));

            fragment->replicas = lappend(fragment->replicas, fhost);
        }

        /* 3. location - fragment meta data */
        struct json_object *js_fragment_metadata;
        if (json_object_object_get_ex(js_fragment, "metadata", &js_fragment_metadata))
            fragment->fragment_md = pstrdup(json_object_get_string(js_fragment_metadata));


        /* 4. userdata - additional user information */
        struct json_object *js_user_data;
        if (json_object_object_get_ex(js_fragment, "userData", &js_user_data))
            fragment->user_data = pstrdup(json_object_get_string(js_user_data));

        /* 5. profile - recommended profile to work with fragment */
        struct json_object *js_profile;
        if (json_object_object_get_ex(js_fragment, "profile", &js_profile))
            fragment->profile = pstrdup(json_object_get_string(js_profile));

        /*
         * HD-2547:
         * Ignore fragment if it doesn't contain any host locations,
         * for example if the file is empty.
         */
        if (fragment->replicas)
            ret_frags = lappend(ret_frags, fragment);
        else
            free_fragment(fragment);

    }

    return ret_frags;
}

/*
 * release memory of a single fragment
 */
void free_fragment(DataFragment *fragment)
{
    ListCell *loc_cell = NULL;

    Assert(fragment != NULL);

    if (fragment->source_name)
        pfree(fragment->source_name);

    foreach(loc_cell, fragment->replicas)
    {
        FragmentHost* host = (FragmentHost*)lfirst(loc_cell);

        if (host->ip)
            pfree(host->ip);
        pfree(host);
    }
    list_free(fragment->replicas);

    if (fragment->fragment_md)
        pfree(fragment->fragment_md);

    if (fragment->user_data)
        pfree(fragment->user_data);

    if (fragment->profile)
        pfree(fragment->profile);

    pfree(fragment);
}

/* Concatenate multiple literal strings using stringinfo */
char* concat(int num_args, ...)
{
    va_list ap;
    StringInfoData str;
    initStringInfo(&str);

    va_start(ap, num_args);

    for (int i = 0; i < num_args; i++) {
        appendStringInfoString(&str, va_arg(ap, char*));
    }
    va_end(ap);

    return str.data;
}