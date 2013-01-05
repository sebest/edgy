from edgy_m import EdgyM
from edgy_r import EdgyR

from pylibmc import Client as Memcache
from redis import Redis


class CsdCompat(object):

    def __init__(self, host='localhost', consolidations=None, mode='redis', *args, **kwargs):
        if not consolidations:
            consolidations = {
                'last_hour': (60, 60),
                'last_day': (60 * 60, 24),
                'last_month': (60 * 60 * 24, 30),
                'last_year': (60 * 60 * 24 * 30, 12),
                'total': (None, None),
                }
        if mode == 'redis':
            self.edgy = EdgyR(Redis(host), consolidations)
        elif mode == 'memcache':
            self.edgy = EdgyM(Memcache([host]), consolidations)
        else:
            raise Exception('Invalid mode %s' % mode)

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
    from time import time

    x = 'x' * 80 + str(time())

    for e in (CsdCompat(mode='memcache'), CsdCompat()):
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
