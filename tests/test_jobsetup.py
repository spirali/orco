import orco
import time


def test_setup_exclusive(env):

    @orco.builder()
    def builder1(c):
        time.sleep(1)

    @orco.builder(job_setup=orco.JobSetup(exclusive=True))
    def builder2(c):
        time.sleep(1)


    runtime = env.test_runtime(n_processes=2)

    start = time.time()
    runtime.compute_many([builder1(1), builder1(2)])
    end = time.time()

    assert 0.99 <= end - start <= 1.5

    start = time.time()
    runtime.compute_many([builder2(1), builder2(2)])
    end = time.time()

    assert 2.0 <= end - start <= 2.5

    start = time.time()
    runtime.compute_many([builder1(3), builder2(4)])
    end = time.time()

    assert 2.0 <= end - start <= 2.5

    start = time.time()
    runtime.compute_many([builder1(5), builder2(6), builder1(7)])
    end = time.time()

    assert 2.0 <= end - start <= 2.5



def test_setup_exclusive2(env):

    @orco.builder()
    def builder1(c):
        pass

    @orco.builder(job_setup=orco.JobSetup(exclusive=True))
    def builder2(c):
        builder1(c * 11)
        builder1(c * 22)
        yield

    @orco.builder()
    def builder3(c):
        builder2(c + 100)
        builder2(c + 1000)
        builder1(c * 44)
        yield

    runtime = env.test_runtime(n_processes=2)
    #runtime.compute_many([builder3(10)])
    runtime.compute_many([builder3(21), builder3(22)])


