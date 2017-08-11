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
#include "postgres.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "pxffragment.h"

static const char *PXF_HOST = "localhost";
static const int PXF_PORT = 51200;

static List* get_data_fragment_list(GPHDUri *hadoop_uri,  ClientContext* client_context);
static void rest_request(GPHDUri *hadoop_uri, ClientContext* client_context, char *rest_msg);
static List* parse_get_fragments_response(List *fragments, StringInfo rest_buf);
static char* concat(int num_args, ...);
static List* filter_fragments_for_segment(List* list);
static void assign_pxf_location_to_fragments(List *fragments);
static void init(GPHDUri* uri, ClientContext* cl_context);
static void free_fragment(DataFragment *fragment);
static void print_fragment_list(List *fragments);
static void init_client_context(ClientContext *client_context);

/* Get List of fragments using PXF
 * Returns selected fragments that have been allocated to the current segment
 */
void set_fragments(GPHDUri* uri) {

	List *data_fragments = NIL;

	/* Context for the Fragmenter API */
	ClientContext client_context;
	PxfInputData inputData = {0};

	Assert(uri != NULL);

	/*
	 * 1. Initialize curl headers
	 */
	init(uri, &client_context);
	if (!uri)
		return;

	/*
	 * Enrich the curl HTTP header
	 */
	inputData.headers = client_context.http_headers;
	inputData.gphduri = uri;
	//inputData.rel = relation;
	build_http_headers(&inputData);

	/*
	 * 2. Get the fragments data from the PXF service
	 */
	data_fragments = get_data_fragment_list(uri, &client_context);

	assign_pxf_location_to_fragments(data_fragments);

	/* debug - enable when tracing */
	if ((FRAGDEBUG >= log_min_messages) || (FRAGDEBUG >= client_min_messages))
		print_fragment_list(data_fragments);

	/*
	 * 3. Call the work allocation algorithm
	 */
	data_fragments = filter_fragments_for_segment(data_fragments);

	uri->fragments = data_fragments;

	return;
}

/*
 * release the fragment list
 */
List*
free_fragment_list(List *fragments)
{
	ListCell *frag_cell = NULL;

	foreach(frag_cell, fragments)
	{
		DataFragment *fragment = (DataFragment*)lfirst(frag_cell);
		free_fragment(fragment);
	}
	list_free(fragments);
	return NIL;
}

/*
 * get_data_fragment_list
 *
 * 1. Request a list of fragments from the PXF Fragmenter class
 * that was specified in the pxf URL.
 *
 * 2. Process the returned list and create a list of DataFragment with it.
 */
static List*
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
static void free_fragment(DataFragment *fragment)
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
static char* concat(int num_args, ...)
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

/*
 * Takes a list of fragments and determines which ones need to be processes by the given segment based on MOD function.
 * Removes the elements which will not be processed from the list and frees up their memory.
 * Returns the resulting list, or NIL if no elements satisfy the condition.
 */
static List*
filter_fragments_for_segment(List* list)
{
    if (!list)
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("internal error in pxfutils.c:filter_list. Parameter list is null.")));

    DistributedTransactionId xid = getDistributedTransactionId();
    if (xid == InvalidDistributedTransactionId)
        ereport(ERROR,
                (errcode_for_file_access(),
                        errmsg("internal error in pxfutils.c:filter_list. Cannot get distributed transaction identifier")));

    /*
     * to determine which segment S should process an element at a given index I, use a randomized MOD function
     * S = MOD(I + MOD(XID, N), N)
     * which ensures more fair work distribution for small lists of just a few elements across N segments
     * global transaction ID is used as a randomizer, as it is different for every query
     * while being the same across all segments for a given query
     */

    List* result = list;
    ListCell *previous = NULL, *current = NULL;

    int index = 0;
    int4 shift = xid % GpIdentity.numsegments;

    for (current = list_head(list); current != NULL; index++)
    {
        if (GpIdentity.segindex == (index + shift) % GpIdentity.numsegments)
        {
            /* current segment is the one that should process, keep the element, adjust cursor pointers */
            previous = current;
            current = lnext(current);
        }
        else
        {
            /* current segment should not process this element, another will, drop the element from the list */
            ListCell* to_delete = current;
            if (to_delete->data.ptr_value)
                free_fragment((DataFragment *) to_delete->data.ptr_value);
            current = lnext(to_delete);
            result = list_delete_cell(list, to_delete, previous);
        }
    }
    return result;
}

/*
 * Preliminary curl initializations for the REST communication
 */
static void init(GPHDUri* uri, ClientContext* cl_context)
{
    char *fragmenter = NULL;
    char *profile = NULL;

    /*
     * 2. Communication with the Hadoop back-end
     *    Initialize churl client context and header
     */
    init_client_context(cl_context);
    cl_context->http_headers = churl_headers_init();

    /* set HTTP header that guarantees response in JSON format */
    churl_headers_append(cl_context->http_headers, REST_HEADER_JSON_RESPONSE, NULL);
    if (!cl_context->http_headers)
        return;

    /*
     * 3. Test that Fragmenter or Profile was specified in the URI
     */
    if(GPHDUri_get_value_for_opt(uri, "fragmenter", &fragmenter, false) != 0
       && GPHDUri_get_value_for_opt(uri, "profile", &profile, false) != 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("FRAGMENTER or PROFILE option must exist in %s", uri->uri)));
    }

    return;
}

/*
 * Assign the remote rest port to each fragment
 */
static void assign_pxf_location_to_fragments(List *fragments)
{
	ListCell *frag_c = NULL;

	foreach(frag_c, fragments)
	{
		ListCell 		*host_c 		= NULL;
		DataFragment 	*fragdata	= (DataFragment*)lfirst(frag_c);
		// char* ip = NULL;

		foreach(host_c, fragdata->replicas)
		{
			FragmentHost *fraghost = (FragmentHost*)lfirst(host_c);
//			/* In case there are several fragments on the same host, we assume
//			 * there are multiple DN residing together.
//			 * The port is incremented by one, to match singlecluster convention */
//			if (pxf_service_singlecluster)
//			{
//				if (ip == NULL)
//				{
//					ip = fraghost->ip;
//				}
//				else if (are_ips_equal(ip, fraghost->ip))
//				{
//					port++;
//				}
//			}
			fraghost->ip = pstrdup(PXF_HOST);
			fraghost->rest_port = PXF_PORT;
		}
	}
}

/*
 * Debug function - print the splits data structure obtained from the namenode
 * response to <GET_BLOCK_LOCATIONS> request
 */
static void
print_fragment_list(List *fragments)
{
	ListCell *fragment_cell = NULL;
	StringInfoData log_str;
	initStringInfo(&log_str);

	appendStringInfo(&log_str, "Fragment list: (%d elements)\n",
			fragments ? fragments->length : 0);

	foreach(fragment_cell, fragments)
	{
		DataFragment	*frag	= (DataFragment*)lfirst(fragment_cell);
		ListCell		*lc		= NULL;

		appendStringInfo(&log_str, "Fragment index: %d\n", frag->index);

		foreach(lc, frag->replicas)
		{
			FragmentHost* host = (FragmentHost*)lfirst(lc);
			appendStringInfo(&log_str, "replicas: host: %s\n", host->ip);
		}
		appendStringInfo(&log_str, "metadata: %s\n", frag->fragment_md ? frag->fragment_md : "NULL");

		if (frag->user_data)
		{
			appendStringInfo(&log_str, "user data: %s\n", frag->user_data);
		}

		if (frag->profile)
		{
			appendStringInfo(&log_str, "profile: %s\n", frag->profile);
		}
	}

	elog(FRAGDEBUG, "%s", log_str.data);
	pfree(log_str.data);
}

/*
 * Initializes the client context
 */
static void init_client_context(ClientContext *client_context)
{
	client_context->http_headers = NULL;
	client_context->handle = NULL;
	initStringInfo(&(client_context->the_rest_buf));
}
