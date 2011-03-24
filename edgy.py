from redis import Redis
from time import time

class Edgy(object):

    def __init__(self, redis, consolidations, prefix='edgy'):
        self.redis = redis
        self.consolidations = consolidations
        self.prefix = prefix

    def update(self, key, amount=1):
        now = int(time())
        for name, (interval, count) in consolidations.items():
            prefix = '%s_%s/%s' % (self.prefix, name, key)
            if not interval:
                self.redis.incr(prefix)
            else:
                k = '%s/%s' % (prefix, now / interval)
                res = self.redis.incr(k, amount)
                if res == amount:
                    self.redis.expire(k, interval * count)

    def get(self, key):
        now = int(time())
        res = {}
        for name, (interval, count) in consolidations.items():
            prefix = '%s_%s/%s' % (self.prefix, name, key)
            if not interval:
                res[name] = self.redis.get(prefix)
            else:
                base = now / interval
                keys = ['%s/%s' % (prefix, base + i) for i in range(1 - count, 1)]
                res[name] = sum([int(i) for i in self.redis.mget(keys) if i])
        return res

if __name__ == '__main__':
    from time import sleep
    consolidations = {
        'last_hour': (60, 60),
        'last_day': (60 * 60, 24),
        'total': (None, None),
        }
    e = Edgy(Redis(), 1, 60)
    s = time()
    for i in range(40000):
        e.update(s)
    print '%.3f' % (time() -s) 

    print e.get(s)

    s = time()
    for i in range(100000):
        e.get(s)
    print '%.3f' % (time() -s) 

    from csd import Csd

    e = Csd()

    s = time()
    for i in range(40000):
        e.update(s, 1)
    print '%.3f' % (time() -s) 

    print e.get(s)

    s = time()
    for i in range(100000):
        e.get(s)
    print '%.3f' % (time() -s) 

