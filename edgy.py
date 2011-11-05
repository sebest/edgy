from redis import Redis
from time import time

class Edgy(object):

    def __init__(self, redis, consolidations, prefix='edgy'):
        self.redis = redis
        self.consolidations = consolidations
        self.prefix = prefix

    def update(self, key, amount=1):
        now = int(time())
        for name, (interval, count) in self.consolidations.items():
            prefix = '%s_%s/%s' % (self.prefix, name, key)
            if not interval:
                self.redis.incr(prefix)
            else:
                k = '%s/%s' % (prefix, now / interval)
                if self.redis.incr(k, amount) == amount:
                    self.redis.expire(k, interval * count)

    def get(self, key, dump=False):
        now = int(time())
        res = {}
        for name, (interval, count) in self.consolidations.items():
            res[name] = self.get_one(name, key, dump, now)
        return res

    def get_one(self, name, key, dump=False, now=None):
        if not now:
            now = int(time())
        interval, count = self.consolidations[name]
        prefix = '%s_%s/%s' % (self.prefix, name, key)
        if not interval:
            return self.redis.get(prefix)
        else:
            base = now / interval
            keys = ['%s/%s' % (prefix, base + i) for i in range(1 - count, 1)]
            if not dump:
                return sum([int(i) for i in self.redis.mget(keys) if i])
            else:
                dbase = base + 1 - count
                return [((dbase + idx)* interval,  int(i)) for idx, i in enumerate(self.redis.mget(keys)) if i]

class CsdCompat(object):

    def __init__(self, host='localhost', port=6379, consolidations=None, *args, **kwargs):
        redis = Redis(host, port)
        if not consolidations:
            consolidations = {
                'last_hour': (60, 60),
                'last_day': (60 * 60, 24),
                'last_month': (60 * 60 * 24, 30),
                'last_year': (60 * 60 * 24 * 30, 12),
                'total': (None, None),
                }
        self.edgy = Edgy(redis, consolidations)

    def __getattr__(self, method):
        def func(*args, **kwargs):
            return getattr(self.edgy, method)(*args, **kwargs)
        setattr(self, method, func)
        return func

    def mupdate(self, keys):
        for args in keys:
            self.update(*args)

    def mget(self, keys):
        dict([(key, self.get(key)) for key in keys])

    def dump(self ,key):
        return self.get(key, dump=True)

    def mdump(self, keys):
        return dict([(key, self.dump(key)) for key in keys])

if __name__ == '__main__':
    from csd import Csd

    x = 'x' * 80 + str(time())

    for e in (Csd(), CsdCompat()):
        print x
        s = time()
        for i in range(40000):
            e.update(x, 1)
        print '%.3f' % ((time() -s) / 40.)

        print e.get(x)

        s = time()
        for i in range(10000):
            e.get(x)
        print '%.3f' % ((time() -s) / 10.)
