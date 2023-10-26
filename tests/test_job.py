from pytest import mark

from zjb.doj.job import GeneratorJob, Job, JobState, generator_job_wrap


@mark.parametrize(
    "func, args, kwargs",
    [
        (sum, (range(5),), {}),
        (max, (3, 5), {}),
        (max, (5, 3), {}),
    ],
)
def test_job_call(func, args, kwargs):
    job = Job(func, *args, **kwargs)

    job()
    assert job.state == JobState.DONE
    assert job.out == func(*args, **kwargs)


def _add(x, y):
    return x + y


def _add_many(xs, ys):
    jobs = []
    for x, y in zip(xs, ys):
        job = Job(_add, x, y)
        jobs.append(job)
        yield job
    return Job(lambda jobs: [job.out for job in jobs], jobs)


def test_generator_job_call():
    xs = [1, 2, 3]
    ys = [5, 6, 7]
    job = GeneratorJob(_add_many, xs, ys)
    job()
    assert job.state == JobState.DONE
    assert job.out == [x + y for x, y in zip(xs, ys)]


def test_generator_job_wrap():
    wrapped_add_many = generator_job_wrap(_add_many)
    xs = [1, 2, 3]
    ys = [5, 6, 7]
    res = [x + y for x, y in zip(xs, ys)]
    assert wrapped_add_many(xs, ys) == res

    job = GeneratorJob(wrapped_add_many, xs, ys)
    job()
    assert job.state == JobState.DONE
    assert job.out == res
