#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

/* Define UNIT_TESTING so that the extension can skip declaring PG_MODULE_MAGIC */
#define UNIT_TESTING

/* include unit under test */
#include "../src/pxffragment.c"

/* include mock files */
#include "mock/pxfutils_mock.c"

/* helper functions */
static List* prepare_list(int fragtotal, int segindex, int segtotal, int xid);

void
test_filter_fragments_for_segment(void **state)
{
    // single segment -- all 3 fragments should be processed by it
    List* list = prepare_list(3, 0, 1, 1);
    List* filtered = filter_fragments_for_segment(list);

    assert_int_equal(filtered->length, 3);
    //assert_int_equal(((DataFragment *)linitial(filtered))->index, 0);
    //assert_int_equal(((DataFragment *)lsecond(filtered)))->index, 1);
    //assert_int_equal(((DataFragment *)lthird(filtered)))->index, 2);

}

static List*
prepare_list(int fragtotal, int segindex, int segtotal, int xid) {
    GpIdentity.segindex = segindex;
    GpIdentity.numsegments = segtotal;

    will_return(getDistributedTransactionId, xid);

    List* result = NIL;

    for (int i=0; i<fragtotal; i++) {
        DataFragment* fragment = palloc0(sizeof(DataFragment));
        fragment->index = index;
        result = lappend(result, fragment);
    }
    return result;
}

int
main(int argc, char* argv[])
{
    cmockery_parse_arguments(argc, argv);

    const UnitTest tests[] = {
            unit_test(test_filter_fragments_for_segment)
    };

    MemoryContextInit();

    return run_tests(tests);
}