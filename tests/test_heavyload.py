import orco


def test_heavyload(env):
    @orco.builder()
    def fast(name, x):
        y = 0
        while y < x * 100:
            y += 1
        return x * len(name)

    @orco.builder()
    def middle(name, size):
        data = [fast(name, i) for i in range(size)]
        yield
        return [x.value for x in data]

    @orco.builder()
    def large(list):
        data = [middle(name, s) for name, s in list]
        yield
        return [max(x.value) for x in data]

    runtime = env.test_runtime()
    print(runtime.compute(large([("abc", 10), ("ab", 100), ("long" * 1000, 30)])).value)
