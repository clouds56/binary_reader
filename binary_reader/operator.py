import struct


class Size:
    ALL = set()
    def __init__(self, size, deps=None):
        if isinstance(size, int):
            self.size = size
            self.size_ = []
        else:
            self.size = 0
            self.size_ = [size]
        self.deps = deps or []

    def __iadd__(self, other):
        self.size += other.size
        self.size_ += other.size_
        self.deps += other.deps
        return self

    @classmethod
    def from_(cls, size):
        if isinstance(size, int):
            return cls(size)
        if isinstance(size, str):
            return cls(struct.Struct(size).size)
        if isinstance(size, Op):
            return cls(size, size.deps)
        if callable(size):
            return cls(size, [Size.ALL])

    @classmethod
    def getdeps(cls, size):
        if isinstance(size, int) or isinstance(size, str):
            return []
        if isinstance(size, Op):
            return size.deps
        if callable(size):
            return Size.ALL

    def __repr__(self):
        return (self.size, self.size_, self.deps).__repr__()

class Op:
    def __init__(self):
        self.funcs = []
        self.deps = []

    def in_(self, range):
        return self._p(lambda x: x in self._get(range), [range])

    def eq(self, rhs):
        return self._p(lambda x: x == self._get(rhs), [rhs])

    def cache(self, name, raw=False):
        self.deps.append(name)
        return self._p(lambda cache: cache[name] if raw else cache[name][2], [])

    def cache_or(self, name, *, raw=False, default=None):
        self.deps.append(name)
        return self._p(lambda cache: self._get(default) if name not in cache else cache[name] if raw else cache[name][2], [default])

    def caches(self, *args):
        def _get(cache, i):
            if isinstance(i, tuple):
                for k in i:
                    cache = cache[k]
            else:
                cache = cache[i]
            return cache
        self.deps += args
        return self._p(self.wrap_failed(lambda cache: [_get(cache, i) for i in args]), [])

    def if_(self, true_cond, false_cond=None):
        return self._p(lambda cond: self._get(true_cond) if cond else self._get(false_cond), [true_cond, false_cond])

    def apply(self, fun, deps=Size.ALL):
        self.deps += deps
        return self._p(lambda result: fun(*[self._get(i) for i in result]), [])

    def apply_(self, fun, deps=Size.ALL):
        self.deps += deps
        return self._p(fun, [])

    def wrap_failed(self, fun):
        def wrapped(*args, **kwargs):
            try:
                return fun(*args, **kwargs)
            except:
                return None

        return wrapped

    def _get(self, i):
        if isinstance(i, Op):
            return i(self.input)
        return i

    def _p(self, func, deps):
        for i in deps:
            if isinstance(i, Op):
                self.deps += Size.getdeps(i)
        self.funcs.append(func)
        return self

    def debug(self, hint=""):
        def _debug(x):
            print("Op(%s): %s" % (hint, x))
            return x

        return self._p(_debug)

    def __call__(self, input):
        self.input = input
        result = input
        for f in self.funcs:
            result = f(result)
            if isinstance(result, Op):
                result = result(input)
            if result is None:
                return None
        return result
