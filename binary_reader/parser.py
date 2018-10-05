import functools
import struct
import enum
import collections
import types

from .operator import Op, Size


def checking(cond, msg):
    # assert cond, msg
    if not cond:
        print(msg)


def with_tuple(t, i, v):
    cls = t.__class__
    if cls == tuple:
        new = cls
    else:
        new = cls._make
    t = list(t)
    t[i] = v
    return new(t)


class Mode(enum.Enum):
    NONE = 0
    STRUCT = 1
    BYTES = 2
    FUNCTION = 3
    DYNAMIC = 4
    LIST = 5

class Struct(struct.Struct):
    keep_array = False

class Schema:
    NA = type(None)
    def __init__(self, name=None, format=None, check=NA, *, mode=None):
        self.name = name
        self.mode = mode
        self.format = format
        self.check = check
        self.size = None
        if isinstance(self.format, tuple) and self.format[0] == "tuple":
            self.__init__(*self.format[1])
            return
        if mode is None:
            self._infer_mode()

    @classmethod
    def infer_mode(cls, format, *, table=None):
        if format is None:
            return Mode.NONE
        elif isinstance(format, int):
            return Mode.BYTES
        elif isinstance(format, str) or isinstance(format, struct.Struct):
            if table is not None and format in table:
                return Mode.FUNCTION
            return Mode.STRUCT
        elif isinstance(format, list):
            return Mode.LIST
        elif isinstance(format, tuple):
            if format[0] == "tuple":
                return cls.infer_mode(format[1])
            elif format[0] == "function":
                return Mode.FUNCTION
            return format[0]
        elif callable(format):
            return Mode.DYNAMIC

    def _infer_mode(self):
        self.mode = Schema.infer_mode(self.format)
        if self.format is None:
            self.size = 0
        elif self.mode == Mode.BYTES:
            self.size = self.format
        elif self.mode == Mode.STRUCT:
            if isinstance(self.format, str):
                self.format = Schema.struct(format)
            self.size = self.format.size
        elif self.mode == Mode.FUNCTION:
            if isinstance(self.format, tuple):
                self.format = Schema.function(self.format)

    def infer_size(self):
        if self.mode == Mode.NONE:
            self.size = Size(0)
        elif self.mode == Mode.BYTES:
            self.size = Size(self.format)
        elif self.mode == Mode.STRUCT:
            if isinstance(self.format, str):
                self.format = Schema.struct(self.format)
            self.size = Size(self.format.size)
        elif self.mode == Mode.LIST:
            size = Size(0)
            for i, v in enumerate(self.format):
                if not isinstance(v, Schema):
                    v = Schema.compile(v)
                    self.format[i] = v
                if v.size is None:
                    v.infer_size()
                size += v.size
                print(v, size)
            self.size = size
        elif self.mode == Mode.DYNAMIC:
            self.size = Size.from_(self.format)
        elif self.mode == "set":
            self.size = Size(0)
        elif self.mode == "read_until_size":
            self.size = Size.from_(self.format[2])
        elif self.mode == "read_until_offset":
            self.size = Size.from_(Op().cache("cur").apply_(lambda x: -x, []))
            self.size += Size.from_(self.format[2])

    @staticmethod
    def struct(s):
        keep_array = False
        if s.startswith("["):
            s = s[1:]
            keep_array = True
        s = Struct(s)
        s.keep_array = keep_array
        return s

    @staticmethod
    def function(f):
        if isinstance(f, tuple) and f[0] == "function":
            f = f[1:]
        if isinstance(f, tuple):
            if len(f) == 2:
                f, args = f
                """Based on http://stackoverflow.com/a/6528148/190597 (Glenn Maynard)"""
                g = type(f)(f.__code__, f.__globals__, name=f.__name__, argdefs=f.__defaults__, closure=f.__closure__)
                g = functools.update_wrapper(g, f)
                g.__kwdefaults__ = f.__kwdefaults__
                g._args = args
                return g
            else:
                f = f[0]
        return f

    @classmethod
    def compile(cls, source, *, name="_", check=NA, table=None):
        mode = None
        if isinstance(source, list):
            format = []
            for i in source:
                c = cls.compile(("tuple", i), table=table)
                c.infer_size()
                print("compile %s -> %s" % (i, c))
                format.append(c)
            source = format
            mode = Mode.LIST
        elif isinstance(source, int):
            mode = Mode.BYTES
        elif isinstance(source, str):
            if table is not None and source in table:
                source = table[source]
                if isinstance(source, tuple):
                    source = ("function", *source)
                else:
                    source = ("function", source)
                return cls.compile(source, name=name, check=check, table=table)
            source = Schema.struct(source)
            mode = Mode.STRUCT
        elif isinstance(source, tuple):
            mode = source[0]
            if mode == "tuple":
                source = source[1]
                name, format = source[:2]
                if len(source) == 3:
                    check = source[2]
                return cls.compile(format, name=name, check=check, table=table)
            elif mode == "function":
                source = cls.function(source)
                mode = Mode.FUNCTION
        elif callable(source):
            mode = Mode.DYNAMIC
        if mode is None:
            print("warning: unknown type %s to compile" % type(source))
        return cls(name, source, check, mode=mode)

    def __repr__(self):
        if self.mode == Mode.LIST:
            return "<binary_reader.parser.Schema name=%s mode=%s size=%s>" % (
                self.name, self.mode, self.size)
        return "<binary_reader.parser.Schema name=%s mode=%s format=%s size=%s>" % (
            self.name, self.mode, self.format, self.size)


Result = collections.namedtuple('Result', ['name', 'cur', 'size', 'value', 'schema'])


def read_schema_raw(mode, format, bin, cur, *, name="_", table=None, cache=None):
    size, result = 0, None

    if mode == Mode.LIST:
        format = [with_tuple(v, 0, ("sub", name, v[0])) if isinstance(v, tuple) else (("sub", name, i), v) for i, v in
                  enumerate(format)]
        result = list(read_schema(format, bin[cur:], table=table, cache=cache, yield_end=True))
        size = result.pop()[1]
        result = [with_tuple(v, 0, v[0][2]) if isinstance(i, tuple) or True else v[3] for i, v in zip(format, result)]
    elif mode == Mode.FUNCTION:
        if table is not None and format in table:
            format = table[format]
        func = Schema.function(format)
        cache[("args",)] = getattr(func, "_args", None)
        try:
            size, result = func(bin, cur, cache)
        except:
            print("unpack failed %s at offset %s" % (i, cur))
        del cache[("args",)]
    elif mode == Mode.BYTES:
        size = format
        result = bin[cur:cur + size]
        if len(result) != size:
            size = len(result)
            print("cannot read enough bytes %s at offset %s (%s)" % ((name, format), cur, size))
    elif mode == Mode.STRUCT:
        keep_array = False
        if isinstance(format, str):
            format = Schema.struct(format)
        if isinstance(format, Struct):
            keep_array = format.keep_array
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
        if isinstance(i, tuple):
            name, format = i[:2]
            check = Schema.NA
            if len(i) == 3:
                check = i[2]
            mode = Schema.infer_mode(format, table=table)
        else:
            mode, name, format, check = i.mode, i.name, i.format, i.check

        while mode == Mode.DYNAMIC:
            format = format(cache)
            mode = Schema.infer_mode(format, table=table)

        size, result = 0, None
        if mode == Mode.NONE:
            continue
        elif isinstance(mode, str) and mode.startswith("read_until"):
            mode = format[0]
            sub_cur = cur
            if mode == "read_until_size":
                format_, to_size = format[1:]
                if callable(to_size):
                    to_size = to_size(cache)
                stop = lambda cache, cur_start=sub_cur: sub_cur - cur_start >= to_size
            elif mode == "read_until_offset":
                format_, offset = format[1:]
                if callable(offset):
                    offset = offset(cache)
                stop = lambda cache: sub_cur >= offset
            size, result = 0, []
            mode_ = Schema.infer_mode(format_, table=table)
            while not stop(cache):
                size_, result_ = read_schema_raw(mode_, format_, bin, sub_cur, table=table, cache=cache)
                size += size_
                result.append(result_)
                sub_cur += size_
        elif isinstance(mode, str) and mode == "set":
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
        elif isinstance(mode, str):
            assert False, "unknown mode %s" % mode
        else:
            size, result = read_schema_raw(mode, format, bin, cur, name=name, table=table, cache=cache)

        if check != Schema.NA:
            if callable(check):
                checking(check(result), "reading %s at offset %d: %s" % (i, cur, result))
            elif check:
                checking(result == check, "reading %s at offset %d: %s" % (i, cur, result))
        if cache is not None:
            cache[name] = (cur, size, result)
            cache[("cur",)] = cur + size

        yield Result(name, cur, size, result, (format, check))
        cur += size
    if yield_end:
        yield Result("_end", cur, 0, None, None)


def read_schema_list_raw(results, *, without_name=False, filter_starts=None):
    for result in results:
        if filter_starts is not None and result.name.startswith(filter_starts):
            continue
        mode, value = None, result.value
        if isinstance(result.schema, Schema):
            mode = result.schema.mode
        elif isinstance(result.schema, tuple):
            if isinstance(result.schema[0], tuple):
                mode = result.schema[0][0]
            elif isinstance(result.schema[0], list):
                mode = Mode.LIST
            elif isinstance(result.schema[0], int):
                mode = Mode.BYTES
        if mode == Mode.LIST:
            value = list(read_schema_list_raw(result.value, without_name=True))
        if without_name:
            yield value
        else:
            yield (result.name, value)


def read_schema_list(schema, bin_, *, table=None, cache=None):
    if cache is None:
        cache = {}
    return read_schema_list_raw(read_schema(schema, bin_, table=table, cache=cache), filter_starts="_")
