"""FoundationDB Spatial Index Layer.

Provides the SpatialIndex() class, an example 2D spatial index for
FoundationDB. It provides efficient queries of the points within an
axis-aligned rectangle by using a z-order fractal curve for
dimensionality reduction.

"""

import fdb
import fdb.tuple

fdb.api_version(23)

verbose = False
printLevels = 3

def xy_to_z(p):
    m = list(p)
    z = 0
    zb = 0
    while m != [0, 0]:
        for i in range(2):
            z += (m[i] & 1) << zb
            zb += 1
            m[i] >>= 1
    return z

def z_to_xy(z):
    m = [0,0]
    l = 0
    while z:
        for i in [0, 1]:
            m[i] += (z & 1) << l
            z >>= 1
        l += 1
    return tuple(m)

## Rect represents an axis-aligned rectangular region
## ul = upper-left point (inclusive)
## lr = lower-right point (exclusive)

class Rect:
    def __init__(self, ul, lr):
        self.ul = ul
        self.lr = lr # exclusive

    def intersect_point(self, p):
        for i in range(2):
            if p[i] < self.ul[i] or p[i] >= self.lr[i]:
                return False
        return True

    def intersect_rect(self, r):
        for i in range(2):
            if self.ul[i] >= r.lr[i] or r.ul[i] >= self.lr[i]:
                return False
        return True

    def __repr__(self):
#        return str(self.ul) + " -> " + str(self.lr)
        out = "\n"
        for y in range(1<<printLevels):
            for x in range(1<<printLevels):
                if self.intersect_point((x,y)):
                    out += "%4d" % xy_to_z((x,y))
                else:
                    out += "   ."
            out += "\n"
        return out

    def size(self):
        return self.lr[0] - self.ul[0]

    # pre: r is aligned
    def larger_aligned_rect(self):
        prev_ul = self.ul
        size = self.lr[0]-self.ul[0]
        ul = (self.ul[0]&~(size*2-1), self.ul[1]&~(size*2-1))
        result = Rect( ul, (ul[0]+size*2,ul[1]+size*2) )
        smaller_z = (result.ul[0] != prev_ul[0]) * 1 + (result.ul[1] != prev_ul[1]) * 2
        return (result, smaller_z)

    # pre: r is aligned, r's size > 1
    # n in [0,4) (z-order)
    def smaller_aligned_rect(self, n):
        size = self.size()
        size /= 2
        x = self.ul[0] + (n&1)*size
        y = self.ul[1] + (n/2)*size
        return Rect((x,y), (x+size, y+size))

    def z_next_intersect_check(self, z):
        max_z = xy_to_z(self.lr)
        for i in range(z, max_z):
            if self.intersect_point(z_to_xy(i)):
                return i
        return None


    def z_next_intersect(self, z):
        max_z = xy_to_z(self.lr)

        p = z_to_xy(z)
        if self.intersect_point(p):
            return z
        r = Rect(p, (p[0]+1, p[1]+1))

        if verbose: print "starting search for next z", r

        # Look for intersections in larger rects with a greater z score
        while True:
            r, z_num = r.larger_aligned_rect()
            if verbose: print "looking for subrect after", z_num, "in larger Rect:", r
            if verbose: print "maxz:%d, size/2:%d" % (max_z, r.size()/2)
            if z_num == 0 and xy_to_z((r.size()/2,0)) > max_z:
                if verbose: print "Aborting: larger rects will not intersect"
                return None

            found = False
            for sri in range(z_num+1, 4):
                sr = r.smaller_aligned_rect(sri)
                if sr.intersect_rect(self):
                    r = sr
                    found = True
                    break
            if found:
                break

        if verbose: print "found >z intersect in ", r
        if r.size() == 1:
            result_z = xy_to_z(r.ul)
            if verbose: print "search complete (A) at", result_z
            return result_z

        while True:
            for sri in range(4):
                if r.size() == 1:
                    result_z = xy_to_z(r.ul)
                    if verbose: print "search complete (B) at", result_z
                    return result_z
                sr = r.smaller_aligned_rect(sri)
                if verbose: print "trying sr", sri
                if xy_to_z(sr.ul) <= z:
                    #if verbose: print "skipping subrect", sri
                    continue
                if sr.intersect_rect(self):
                    r = sr
                    if verbose: print "found subrect", sri, "intersect", r
                    break
            else:
                return None

    def z_next_intersect_validate(self, z):
        r1 = self.z_next_intersect_check(z)
        r2 = self._z_next_intersect(z)
        if r1 != r2:
            print "next(",z,") ==",r1,"but reports", r2
            assert False
        return r2

###################################
# This defines a Subspace of keys #
###################################

class Subspace (object):
    def __init__(self, prefixTuple, rawPrefix=""):
        self.rawPrefix = rawPrefix + fdb.tuple.pack(prefixTuple)
    def __getitem__(self, name):
        return Subspace( (name,), self.rawPrefix )
    def key(self):
        return self.rawPrefix
    def pack(self, tuple):
        return self.rawPrefix + fdb.tuple.pack( tuple )
    def unpack(self, key):
        assert key.startswith(self.rawPrefix)
        return fdb.tuple.unpack(key[len(self.rawPrefix):])
    def range(self, tuple=()):
        p = fdb.tuple.range( tuple )
        return slice(self.rawPrefix + p.start, self.rawPrefix + p.stop)

################
# SpatialIndex #
################

## SpatialIndex associates an identifier (key) with an non-negative
## integral 2D location and allows efficiently finding all such keys
## within a rectangular 2D region
##
## By creating SpatialIndex with a Subspace, all kv pairs modified by the 
## layer will have keys that start within that Subspace.

class SpatialIndex():
    def __init__(self, subspace):
        self.subspace = subspace
        self.z_key = subspace["Z"]
        self.key_z = subspace["K"]

    def validLocation(self, p):
        if not isinstance(p, tuple): return False
        if len(p) != 2: return False
        for x in p:
            if not isinstance(x, int): return False
            if x < 0: return False
        return True

    @fdb.transactional
    def clear(self, tr):
        del tr[self.subspace.range()]

    @fdb.transactional
    def set_location(self, tr, key, pos):
        assert self.validLocation(pos)
        z = xy_to_z(pos)
        existing_z = list(tr[self.key_z[key].range()])
        if len(existing_z):
            oldz = self.key_z.unpack(existing_z[0].key)[-1]
            del tr[self.key_z.pack((key, oldz))]
            del tr[self.z_key.pack((oldz, key))]

        tr[self.z_key.pack((z, key))] = ''
        tr[self.key_z.pack((key, z))] = ''

    @fdb.transactional
    def get_location(self, tr, key):
        found_z = list(tr[self.key_z[key].range()])
        if len(found_z):
            z = self.key_z.unpack(found_z[0].key)[-1]
            return z_to_xy(z)
        else:
            return None

    # returns a list of (key, pos) pairs
    @fdb.transactional
    def get_in_rectangle(self, tr, r):
        for i in range(2):
            if r.lr[i] == r.ul[i]:
                return []
        results = []
        z = r.z_next_intersect(0)
        while z is not None:
            res = tr[self.z_key.pack((z,)):
                     self.z_key.range().stop]
            if verbose: print "query from", z, "-> end"
            done = True
            for k, v in res:
                foundz = self.z_key.unpack(k)[-2]
                foundkey = self.z_key.unpack(k)[-1]
                xy = z_to_xy(foundz)
                if not r.intersect_point(xy):
                    z = r.z_next_intersect(foundz)
                    if verbose: print "advancing from z =", foundz, "to z =", z
                    done = False
                    break
                if verbose: print "adding", foundkey, "to results"
                results.append((foundkey, xy))
            if done: break
        return results


def z_print():
    for y in range(1<<printLevels):
        for x in range(1<<printLevels):
            assert z_to_xy(xy_to_z((x,y))) == (x,y)
            print "%4d" % xy_to_z((x, y)) ,
        print ""


import random

##################
# internal tests #
##################
def ri(): return random.randint(0, 8)

def internal_test1():
    for n in range(1000):
        i = ri()
        j = ri()
        assert xy_to_z((i,j)) == xy_to_z((i,j)), "fail on %d %d" % (i, j)

def internal_test2():
    del db[:]
    print "cleared"

    ris = [ri() for x in range(4)]
    r = Rect( (min(ris[0], ris[2]), min(ris[1], ris[3])),
              (max(ris[0], ris[2]), max(ris[1], ris[3])))

    print "testing Rect", r

    ip = []
    for n in range(10):
        p = (ri(), ri())
        if verbose: print "adding %d to (%d, %d)" % (n, p[0], p[1])
        key = '%d' % n
        if r.intersect_point(p): ip.append(key)
        s.set_location(db, key, p)

    results = s.get_in_rectangle(db, r)
    a = sorted([x[0] for x in results])
    b = sorted(ip)
    print a
    print b
    assert a==b

##############################
# Spatial Index sample usage #
##############################

# caution: modifies the database!
def spatial_example():
    db = fdb.open()
    index_location = Subspace( ('s_index', ) )
    s = SpatialIndex( index_location )

    s.clear(db)

    print "point d is at", s.get_location(db, 'd')

    s.set_location(db, 'a', (3,2))
    s.set_location(db, 'b', (1,4))
    s.set_location(db, 'c', (5,3))
    s.set_location(db, 'd', (2,3))
    s.set_location(db, 'e', (0,0))

    print "point d is at", s.get_location(db, 'd')

    print "Searching in rectangle (1,1) -> (5,5):"
    print s.get_in_rectangle(db, Rect((1,1), (5,5)))

if __name__ == "__main__":
    spatial_example()
