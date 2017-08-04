/*
 * Copyright (c) 2011 EMC Corporation All Rights Reserved
 *
 * This software is protected, without limitation, by copyright law
 * and international treaties. Use of this software and the intellectual
 * property contained therein is expressly limited to the terms and
 * conditions of the License Agreement under which it is provided by
 * or on behalf of EMC.
 *
 * ---------------------------------------------------------------------
 *
 * This is a replacement of the quicklz_compression.c file to using lz4.
 *
 * Note that other compression algorithms actually work better for new
 * installations anyway.
 */

#include "postgres.h"
#include "storage/gp_compress.h"
#include "utils/builtins.h"
#include "utils/lz4_1_7_5.h"

/* Internal state for lz4 */
typedef struct lz4_state
{
    bool compress;		/* compress or decompress? */

    /*
     * The compression and decompression functions.
     */
    int (*compress_fn) (Bytef *dest,
                        uLongf *destLen,
                        const Bytef *source,
                        uLong sourceLen);

    int (*decompress_fn) (Bytef *dest,
                          uLongf *destLen,
                          const Bytef *source,
                          uLong sourceLen);
} lz4_state;

static size_t
desired_sz(size_t input)
{
    return LZ4_COMPRESSBOUND(input);
}

Datum
lz4_constructor(PG_FUNCTION_ARGS)
{
    /* PG_GETARG_POINTER(0) is TupleDesc that is currently unused.
     * It is passed as NULL.
     *
     * The compression level is ignored since using the default lz4
     * compression function. */
    StorageAttributes *sa = PG_GETARG_POINTER(1);
    CompressionState  *cs = palloc0(sizeof(CompressionState));
    lz4_state *state = palloc0(sizeof(lz4_state));
    bool compress = PG_GETARG_BOOL(2);

    cs->opaque = (void *) state;
    cs->desired_sz = desired_sz;

    Insist(PointerIsValid(sa->comptype));

    state->compress = compress;
    state->compress_fn = LZ4_compress_default;
    state->decompress_fn = LZ4_decompress_safe;

    PG_RETURN_POINTER(cs);
}

Datum
lz4_destructor(PG_FUNCTION_ARGS)
{
    CompressionState *cs = PG_GETARG_POINTER(0);

    if (cs != NULL && cs->opaque != NULL) {
        pfree(cs->opaque);
    }

    PG_RETURN_VOID();
}

Datum
lz4_compress(PG_FUNCTION_ARGS)
{
    const void *src = PG_GETARG_POINTER(0);
    int32 src_sz = PG_GETARG_INT32(1);
    void *dst = PG_GETARG_POINTER(2);
    int32 dst_sz = PG_GETARG_INT32(3);
    int32 *dst_used = PG_GETARG_POINTER(4);
    CompressionState *cs = (CompressionState *) PG_GETARG_POINTER(5);
    lz4_state *state = (lz4_state *) cs->opaque;

    *dst_used = state->compress_fn((const char *)src, (unsigned char *)dst,
                                   src_sz, dst_sz);
    if (!*dst_used) {
        elog(ERROR, "lz4 compression failed");
    }

    PG_RETURN_VOID();
}

Datum
lz4_decompress(PG_FUNCTION_ARGS)
{
    const char *src = PG_GETARG_POINTER(0);
    int32  src_sz = PG_GETARG_INT32(1);
    void  *dst = PG_GETARG_POINTER(2);
    int32  dst_sz = PG_GETARG_INT32(3);
    int32 *dst_used = PG_GETARG_POINTER(4);
    CompressionState *cs = (CompressionState *) PG_GETARG_POINTER(5);
    lz4_state *state = (lz4_state *) cs->opaque;

    Insist(src_sz > 0 && dst_sz > 0);

    *dst_used = state->decompress_fn((const char *)src, dst, src_sz, dst_sz);
    if (!*dst_used) {
        elog(ERROR, "lz4 decompression failed");
    }

    PG_RETURN_VOID();
}

Datum
lz4_validator(PG_FUNCTION_ARGS)
{
    PG_RETURN_VOID();
}
