import statistics
import time


class Bench:
    name = None
    driver = None
    queries = []

    def __call__(self):
        try:
            self.setup()
        except ImportError:
            print(f'Driver {self.driver} not installed, skipping…')
            return

        for query in self.queries:
            self.time(query)

        self.close()

    def time(self, query, iterations=10):
        times = [None] * iterations

        print(query)
        for i in range(iterations):
            start = time.perf_counter()
            self.run(query)
            times[i] = time.perf_counter() - start

        median = statistics.median(times)
        mean = statistics.mean(times)
        max_ = max(times)
        min_ = min(times)
        print(
            f"Mean: {mean:.4f} - "
            f"Median: {median:.4f} - "
            f"Max: {max_:.4f} - "
            f"Min: {min_:.4f}"
        )
        print('---')

    def setup(self):
        raise NotImplementedError()

    def run(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()


class SlonikMixin:
    driver = 'slonik'
    conn = None

    def setup(self):
        import slonik

        self.conn = slonik.Connection.from_env()

        # warmup the connection
        self.conn.get_one('SELECT 1')

    def close(self):
        self.conn.close()


class PsycopgMixin:
    driver = 'psycopg2'
    conn = None

    def setup(self):
        import psycopg2

        self.conn = psycopg2.connect(host=None)
        self.cur = self.conn.cursor()

        # warmup the connection
        self.cur.execute('SELECT 1')
        self.cur.fetchone()

    def close(self):
        self.cur.close()
        self.conn.close()


class AsyncpgMixin:
    driver = 'asyncpg'
    conn = None
    loop = None

    def setup(self):
        import asyncio
        import asyncpg

        self.loop = asyncio.get_event_loop()
        self.conn = self.loop.run_until_complete(asyncpg.connect())

        # warmup the connection
        self.loop.run_until_complete(self.conn.fetch('SELECT 1'))

    def close(self):
        self.loop.run_until_complete(self.conn.close())


# --- One row, several columns ---


class ColumnsBench(Bench):
    name = 'columns'
    queries = [
        "SELECT 1",
        "SELECT 1, 'foo', 42.21::float",
        "SELECT '3d9d291d-8668-480f-98bf-46ee10d07a5d'::uuid",
    ]


class SlonikColumnsBench(SlonikMixin, ColumnsBench):

    def run(self, query):
        self.conn.get_one(query)


class PsycopgColumnsBench(PsycopgMixin, ColumnsBench):

    def run(self, query):
        self.cur.execute(query)


class AsyncpgColumnsBench(AsyncpgMixin, ColumnsBench):

    def run(self, query):
        self.loop.run_until_complete(self.conn.fetch(query))


# --- Scrolling values ---


class ScrollingBench(Bench):
    name = 'scrolling'
    queries = [
        'SELECT generate_series(1, 100)',
        'SELECT generate_series(1, 1000)',
        'SELECT generate_series(1, 10000)',
    ]


class SlonikScrollingBench(SlonikMixin, ScrollingBench):

    def run(self, query):
        for result in self.conn.query(query):
            pass


class PsycopgScrollingBench(PsycopgMixin, ScrollingBench):

    def run(self, query):
        self.cur.execute(query)
        for result in self.cur:
            pass


class AsyncpgScrollingBench(AsyncpgMixin, ScrollingBench):

    def run(self, query):
        # FIXME: can we scroll the results instead?
        self.loop.run_until_complete(self.conn.fetch(query))


# ------


if __name__ == '__main__':
    benches = [
        ColumnsBench,
        ScrollingBench,
    ]

    for bench in benches:
        print("-" * 80)
        print(f"Running benchmark {bench.name}")

        for cls in bench.__subclasses__():
            print()
            print(f"With {cls.driver}")
            print()
            instance = cls()
            instance()
