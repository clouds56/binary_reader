import struct


def checking(cond, msg):
    # assert cond, msg
    if not cond:
        print(msg)


def with_tuple(t, i, v):
    t = list(t)
    t[i] = v
    return tuple(t)


def read_schema_raw(format, bin, cur, *, name="_", table=None, cache=None):
    size, result = 0, None
    if isinstance(format, list):
        format = [with_tuple(v, 0, ("sub", name, v[0])) if isinstance(v, tuple) else (("sub", name, i), v) for i, v in
                  enumerate(format)]
        result = list(read_schema(format, bin[cur:], table=table, cache=cache, yield_end=True))
        size = result.pop()[1]
        result = [with_tuple(v, 0, v[0][2]) if isinstance(i, tuple) or True else v[3] for i, v in zip(format, result)]
    elif format in table:
        try:
            size, result = table[format](bin, cur, cache)
        except:
            print("unpack failed %s at offset %s" % ((name, format), cur))
    elif isinstance(format, int):
        size = format
        result = bin[cur:cur + size]
        if len(result) != size:
            size = len(result)
            print("cannot read enough bytes %s at offset %s (%s)" % ((name, format), cur, size))
    elif isinstance(format, str):
        keep_array = False
        if format.startswith("["):
            format = format[1:]
            keep_array = True
        format = struct.Struct(format)
        size = format.size
        result = []
        try:
            result = format.unpack(bin[cur:cur + size])
        except:
            print("unpack failed %s at offset %s" % ((name, format), cur))
        if not keep_array:
            if len(result) == 0:
                result = None
            elif len(result) == 1:
                result = result[0]
            else:
                result = list(result)
    return size, result


def read_schema(schema, bin, *, table=None, cache=None, yield_end=False):
    cur = 0
    if table is None:
        table = {}
    for i in schema:
        name, format = i[:2]
        if callable(format):
            format = format(cache)

        size, result = 0, None
        if format is None:
            continue
        elif isinstance(format, tuple) and format[0].startswith("read_until"):
            mode = format[0]
            sub_cur = cur
            if mode == "read_until_size":
                format, to_size = format[1:]
                if callable(to_size):
                    to_size = to_size(cache)
                stop = lambda cache, cur_start=sub_cur: sub_cur - cur_start >= to_size
            elif mode == "read_until_offset":
                format, offset = format[1:]
                if callable(offset):
                    offset = offset(cache)
                stop = lambda cache: sub_cur >= offset
            size, result = 0, []
            while not stop(cache):
                size_, result_ = read_schema_raw(format, bin, sub_cur, table=table, cache=cache)
                size += size_
                result.append(result_)
                sub_cur += size_
        elif isinstance(format, tuple) and format[0] == "function":
            args = None
            if len(format) > 2:
                args = format[2]
            format = format[1]
            cache[("args",)] = args
            try:
                size, result = format(bin, cur, cache)
            except:
                print("unpack failed %s at offset %s" % (i, cur))
            del cache[("args",)]
        elif isinstance(format, tuple) and format[0] == "set":
            size, result = 0, format[1]
            if callable(result):
                args = None
                if len(format) > 2:
                    args = format[2]
                cache[("args",)] = args
                result = result(cache)
                del cache[("args",)]
            if result is None:
                continue
        elif isinstance(format, tuple):
            assert False, "unknown mode %s" % format[0]
        else:
            size, result = read_schema_raw(format, bin, cur, name=name, table=table, cache=cache)

        if len(i) == 3:
            check = i[2]
            if callable(check):
                checking(check(result), "reading %s at offset %d: %s" % (i, cur, result))
            else:
                checking(result == check, "reading %s at offset %d: %s" % (i, cur, result))
        if cache is not None:
            cache[name] = (cur, size, result)
            cache[("cur",)] = cur + size

        if isinstance(format, list):
            yield (("list", name), cur, size, result)
        else:
            yield (name, cur, size, result)
        cur += size
    if yield_end:
        yield ("_end", cur, 0, None)


def read_schema_list_raw(result, *, without_name=False, filter_starts=None):
    for name, cur, size, result in result:
        if isinstance(name, tuple):
            if name[0] == "list":
                result = list(read_schema_list_raw(result, without_name=True))
            name = name[1]
        if filter_starts is not None and name.startswith(filter_starts):
            continue
        if without_name:
            yield result
        else:
            yield (name, result)


def read_schema_list(schema, bin_, *, table=None, cache=None):
    for i in schema:
        if callable(i[1]) or isinstance(i[1], tuple) or isinstance(i[1], list):
            if cache is None:
                cache = {}
            break
    return read_schema_list_raw(read_schema(schema, bin_, table=table, cache=cache), filter_starts="_")
