class Op:
    def __init__(self):
        self.funcs = []

    def in_(self, range):
        return self._p(lambda x: x in self._get(range))

    def eq(self, rhs):
        return self._p(lambda x: x == self._get(rhs))

    def cache(self, name, raw=False):
        return self._p(lambda cache: cache[name] if raw else cache[name][2])

    def cache_or(self, name, *, raw=False, default=None):
        return self._p(lambda cache: default if name not in cache else cache[name] if raw else cache[name][2])

    def caches(self, *args):
        def _get(cache, i):
            if isinstance(i, tuple):
                for k in i:
                    cache = cache[k]
            else:
                cache = cache[i]
            return cache

        return self._p(self.wrap_failed(lambda cache: [_get(cache, i) for i in args]))

    def if_(self, true_cond, false_cond=None):
        return self._p(lambda cond: self._get(true_cond) if cond else self._get(false_cond))

    def apply(self, fun):
        return self._p(lambda result: fun(*[self._get(i) for i in result]))

    def apply_(self, fun):
        return self._p(fun)

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

    def _p(self, func):
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
