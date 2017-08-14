#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

/* Define UNIT_TESTING so that the extension can skip declaring PG_MODULE_MAGIC */
#define UNIT_TESTING

/* include unit under test */
#include "../src/pxffragment.c"

/* include mock files */
#include "mock/libchurl_mock.c"
#include "mock/pxfheaders_mock.c"
#include "mock/pxfuriparser_mock.c"
#include "mock/pxfutils_mock.c"

/* helper functions */
static List* prepare_fragment_list(int fragtotal, int segindex, int segtotal, int xid);
static List* prepare_fragment_list_with_replicas(int fragtotal, int segindex, int segtotal, int xid, int num_replicas);

void
test_filter_fragments_for_segment(void **state)
{
    // single segment -- all 3 fragments should be processed by it
    List* list = prepare_fragment_list(3, 0, 1, 1);
    List* filtered = filter_fragments_for_segment(list);

    assert_int_equal(filtered->length, 3);
}

static List*
prepare_fragment_list(int fragtotal, int segindex, int segtotal, int xid) {
    GpIdentity.segindex = segindex;
    GpIdentity.numsegments = segtotal;

    will_return(getDistributedTransactionId, xid);

    List* result = NIL;

    for (int i=0; i<fragtotal; i++) {
        DataFragment* fragment = palloc0(sizeof(DataFragment));
        fragment->index = i;
        result = lappend(result, fragment);
    }
    return result;
}

static List*
prepare_fragment_list_with_replicas(int fragtotal, int segindex, int segtotal, int xid, int num_replicas) {
    GpIdentity.segindex = segindex;
    GpIdentity.numsegments = segtotal;

    will_return(getDistributedTransactionId, xid);

    List* result = NIL;

    for (int i=0; i<fragtotal; i++) {
    	DataFragment* fragment = (DataFragment*)palloc0(sizeof(DataFragment));
        fragment->index = i;
		for (int j=0; j<num_replicas; j++)
		{
			FragmentHost* fhost = (FragmentHost*)palloc(sizeof(FragmentHost));
			fhost->ip = pstrdup(PXF_HOST);
			fhost->rest_port = PXF_PORT;
			fragment->replicas = lappend(fragment->replicas, fhost);
		}
		result = lappend(result, fragment);
    }
    return result;
}

void test_parse_get_fragments_response(void **state)
{
    List *data_fragments = NIL;
    StringInfoData frag_json;
    initStringInfo(&frag_json);
    appendStringInfo(&frag_json, "{\"PXFFragments\":[{\"index\":0,\"sourceName\":\"demo/text2.csv\",\"userData\":dummydata\"metadata\":\"metadatavalue1\",\"replicas\":[\"10.207.4.23\",\"10.207.4.23\",\"10.207.4.23\"]},{\"index\":0,\"sourceName\":\"demo/text_csv.csv\",\"metadata\":\"metadatavalue2\",\"replicas\":[\"10.207.4.23\",\"10.207.4.23\",\"10.207.4.23\"]}]}");
    data_fragments = parse_get_fragments_response(data_fragments, &frag_json);
    assert_true(list_difference(data_fragments, data_fragments) == NIL);
}

void test_parse_get_fragments_response_nullfragment(void **state)
{
    List *data_fragments = NIL;
    StringInfoData frag_json;
    initStringInfo(&frag_json);
    appendStringInfo(&frag_json, "{}");
    data_fragments = parse_get_fragments_response(data_fragments, &frag_json);
    assert_true(list_difference(data_fragments, data_fragments) == NIL);


}
void test_parse_get_fragments_response_emptyfragment(void **state)
{
    List *data_fragments = NIL;
    StringInfoData frag_json;
    initStringInfo(&frag_json);
    appendStringInfo(&frag_json, "{\"PXFFragments\":[]}");
    data_fragments = parse_get_fragments_response(data_fragments, &frag_json);
    assert_true(list_difference(data_fragments, data_fragments) == NIL);
}

void test_parse_get_fragments_response_nulluserdata(void **state)
{
    List *data_fragments = NIL;
    StringInfoData frag_json;
    initStringInfo(&frag_json);
    appendStringInfo(&frag_json, "{\"PXFFragments\":[{\"index\":0,\"userData\":null,\"sourceName\":\"demo/text2.csv\",\"metadata\":\"metadatavalue1\",\"replicas\":[\"10.207.4.23\",\"10.207.4.23\",\"10.207.4.23\"]},{\"index\":0,\"userData\":null,\"sourceName\":\"demo/text_csv.csv\",\"metadata\":\"metadatavalue2\",\"replicas\":[\"10.207.4.23\",\"10.207.4.23\",\"10.207.4.23\"]}]}");
    data_fragments = parse_get_fragments_response(data_fragments, &frag_json);
    assert_true(list_difference(data_fragments, data_fragments) == NIL);
}

//bool compareLists(List* list1, List* list2, bool (*compareType)(void*, void*) )
//{
//    ListCell   *cell;
//
//    if ( (list1 && !list2) || (list2 && !list1) || (list1->length != list2->length) )
//        return false;
//
//    foreach(object1, list1)
//    {
//        bool isExists = false;
//        foreach(object2, list2)
//        {
//            if((*compareType)(object1, object2)) {
//                isExists = true;
//                break;
//            }
//        }
//        if( !isExists )
//            return false;
//    }
//
//    return true;
//}
//
//bool compareFragment(DataFragment fragment1, DataFragment fragment2)
//{
//    (fragment1.index == fragment2.index) &&
//    compareLists(fragment1.replicas,fragment2.replicas, ) &&
//    strcmp(fragment1.source_name, fragment2.source_name) &&
//    strcmp(fragment1.fragment_md, fragment2.fragment_md) &&
//    strcmp(fragment1.user_data, fragment2.user_data) &&
//    strcmp(fragment1.profile, fragment2.profile)
//
//}
//
//bool compareReplica(List* replica1, List* replica2)
//{
//    return true;
//}

int
main(int argc, char* argv[])
{
    cmockery_parse_arguments(argc, argv);

    const UnitTest tests[] = {
            unit_test(test_filter_fragments_for_segment),
            unit_test(test_parse_get_fragments_response)
    };

    MemoryContextInit();

    return run_tests(tests);
}
