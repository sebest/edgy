from time import time
from pylibmc import NotFound


class EdgyM(object):

    def __init__(self, mc, consolidations, prefix='edgy'):
        self.mc = mc
        self.consolidations = consolidations
        self.prefix = prefix

    def update(self, key, amount=1):
        now = int(time())
        for name, (interval, count) in self.consolidations.items():
            prefix = '%s_%s%s' % (self.prefix, name, key)
            if not interval:
                try:
                    self.mc.incr(prefix, amount)
                except NotFound:
                    self.mc.set(prefix, amount)
            else:
                k = '%s%s' % (prefix, now / interval)
                try:
                    self.mc.incr(k, amount)
                except NotFound:
                    expire = interval * count if interval * count < 2592000 else 0
                    self.mc.set(k, amount, time=expire)

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
        prefix = '%s_%s%s' % (self.prefix, name, key)
        if not interval:
            return self.mc.get(prefix)
        else:
            base = now / interval
            keys = [str(base + i) for i in range(1 - count, 1)]
            if not dump:
                return sum([int(i) for i in self.mc.get_multi(keys, key_prefix=prefix).values() if i])
            else:
                dbase = base + 1 - count
                return [((dbase + idx)* interval,  int(i)) for idx, i in enumerate(self.mc.get_multi(keys, key_prefix=prefix).values()) if i]
