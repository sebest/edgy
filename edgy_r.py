from time import time


class EdgyR(object):

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
            return int(self.redis.get(prefix))
        else:
            base = now / interval
            keys = ['%s/%s' % (prefix, base + i) for i in range(1 - count, 1)]
            if not dump:
                return sum([int(i) for i in self.redis.mget(keys) if i])
            else:
                dbase = base + 1 - count
                return [((dbase + idx)* interval,  int(i)) for idx, i in enumerate(self.redis.mget(keys)) if i]
