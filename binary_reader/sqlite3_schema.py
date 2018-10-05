from .operator import Op as sf
from .parser import *

# https://www.sqlite.org/fileformat2.html
# file[0:100]
header_schema = [
    ("_magic", 16, b"SQLite format 3\000"),
    #16
    ("page_size", "!H", sf().in_(range(512, 32768+1))),
    ("write_version", "b"),
    ("read_version", "b"),
    #20
    ("reverse_space", "b"),
    ("_64", "b", 64),
    ("_32", "b", 32),
    ("_32", "b", 32),
    #24
    ("change_counter", "!l"),
    ("in_header_db_size", "!l"),
    #32
    ("first_freelist_page", "!l"),
    ("total_freelist_page", "!l"),
    ("schema_cookie", "!l"),
    ("schema_format", "!l", sf().in_(range(1,5))),
    ("page_cache_size", "!l"),
    ("largest_btree", "!l"),
    ("text_encoding", "!l"),
    ("user_version", "!l"),
    ("incremental_vacuum", "!l"),
    ("application_id", "!l"),
    ("_reversed", 20, lambda r: len([i for i in r if i != 0]) == 0),
    ("valid_for", "!l"),
    ("version", "!l"),
]

# page[0:12]
page_header_schema = [
    ("page_type", "b", sf().in_([2, 5, 10, 13])),
    ("first_freeblock", "!H"),
    ("cell_number", "!H"),
    ("cell_start_offset", "!H"),
    ("cell_free_bytes", "b"),
    ("right_most_page", sf().cache("page_type").in_([2,5]).if_("!L")),
    ("cell_offset_array", sf().cache("cell_number").apply_(lambda r: "[!"+"H"*r, []))
]

def local_payload_size(page_type, payload_size, page_size):
    # https://github.com/mackyle/sqlite/blob/a419afd73a544e30df878db55f7faa17790c01bd/tool/showdb.c#L370
    # static i64 localPayload(i64 nPayload, char cType)
    if page_type not in [2, 10, 13]:
        return
    P = payload_size
    U = page_size
    X = U-35 if page_type == 13 else ((U-12)*64//255)-23
    M = ((U-12)*32//255)-23
    K = M+((P-M)%(U-4))
    if P <= X:
        P0 = P
    elif K <= X:
        P0 = K
    else:
        P0 = M
    #print("page_size(%s,%s,%s,%s): %s -> %s" % (U, X, M, K, P, P0))
    # page_size:(4096,4061,489,4002) 4002 -> 4002
    return P0

cell_header_schema = [
    ("left_child_page", sf().cache("page_type",True).in_([2,5]).if_("!L")),
    ("payload_size", sf().cache("page_type",True).in_([2,10,13]).if_("varint")),
    ("local_payload_size", ("set", sf().caches("page_type", ("payload_size", 2), ("config", "page_size")).apply(local_payload_size))),
    ("rowid", sf().cache("page_type",True).in_([5,13]).if_("varint")),
    ("payload", sf().cache_or("local_payload_size")),
    ("overflow_page", sf().cache("page_type",True).in_([2, 10, 13]).if_(sf().cache("payload_size").eq(sf().cache("local_payload_size"))).if_(None, "!l")), # omitted if not used
    # sf().caches(("local_payload_size",2), ("payload_size",2)).apply(lambda a,b: a==b)
]

block_header_schema = [
    ("next_offset", "!H"),
    ("block_size", "!H"),
]

record_format_schema = [
    ("header_size", "varint"),
    ("column_types", ("read_until_offset", "varint", sf().cache("header_size"))),
    ("column_contents", lambda cache: [(i, ("function", read_variable, v)) for i, v in enumerate(cache["column_types"][2])]),
]

page_overflow_header_schema = [
    ("next_page", "!L"),
    ("offset", ("set", sf().cache(("cur",), True))),
]


def read_varint(bin, cur, _):
    # for sqlite3: https://www.sqlite.org/fileformat2.html#varint
    # for sqlite4: https://sqlite.org/src4/doc/trunk/www/varint.wiki
    x = 0
    for i in range(8):
        c = bin[cur + i]
        if c < 128:
            return i + 1, x * 128 + c
        x = x * 128 + c - 128
    return 9, x * 256 + bin[cur + 8]


def read_fixint(bin, cur, size):
    x, = struct.unpack("b", bin[cur:cur + 1])
    for i in range(1, size):
        x = x * 256 + bin[cur + i]
    return x


def read_variable(bin, cur, cache):
    serial_type = cache[("args",)]
    assert (serial_type >= 0)
    if serial_type == 0:
        size, value = 0, None
    elif serial_type <= 6:
        if serial_type <= 4:
            size = serial_type
        elif serial_type == 5:
            size = 6
        elif serial_type == 6:
            size = 8
        value = read_fixint(bin, cur, size)
    elif serial_type == 7:
        size = 8
        value, = struct.unpack("!d", bin[cur:cur + size])
    elif serial_type <= 9:
        size, value = 0, serial_type % 2
    elif serial_type >= 12:
        if serial_type % 2 == 0:
            size = (serial_type - 12) // 2
        else:
            size = (serial_type - 13) // 2
        # print("read_variable %s %s at %s" % (serial_type, size, cur))
        value = bin[cur:cur + size]
        if len(value) != size:
            print("cannot read enough bytes at offset %s (%s, %s)" % (cur, size, len(value)))
            size = len(value)
        if serial_type % 2 == 1:
            value = value.decode()
    return size, value


format_table = {
    "varint": read_varint,
    "variable": read_variable,
}

schemas = {
    "header_schema": Schema.compile(header_schema, name="header", table=format_table),
    "page_header_schema": Schema.compile(page_header_schema, name="page_header", table=format_table),
    "cell_header_schema": Schema.compile(cell_header_schema, name="cell_header", table=format_table),
    "block_header_schema": Schema.compile(block_header_schema, name="block_header", table=format_table),
    "record_format_schema": Schema.compile(record_format_schema, name="record_format", table=format_table),
    "page_overflow_header_schema": Schema.compile(page_overflow_header_schema, name="page_overflow_header", table=format_table),
}
