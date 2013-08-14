###################################
# This defines a Subspace of keys #
###################################

import fdb.tuple

class Subspace (object):
    def __init__(self, prefixTuple=tuple(), rawPrefix=""):
        self.rawPrefix = rawPrefix + fdb.tuple.pack(prefixTuple)

    def __repr__(self):
        return 'Subspace(rawPrefix=' + repr(self.rawPrefix) + ')'

    def __getitem__(self, name):
        return Subspace( (name,), self.rawPrefix )
   
    def key(self):
        return self.rawPrefix

    def pack(self, t = tuple()):
        return self.rawPrefix + fdb.tuple.pack( t )
    
    def unpack(self, key):
        assert key.startswith(self.rawPrefix)
        return fdb.tuple.unpack(key[len(self.rawPrefix):])
    
    def range(self, t = tuple()):
        p = fdb.tuple.range( t )
        return slice(self.rawPrefix + p.start, self.rawPrefix + p.stop)

    def contains(self, key):
        return key.startswith(self.rawPrefix)

    def as_foundationdb_key(self):
        return self.rawPrefix

    def subspace(self, tuple):
        return Subspace( tuple, self.rawPrefix )
