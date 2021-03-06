"""FoundationDB String Interning Layer.

Provides the StringIntern() class for interning (aka normalizing,
aliasing) commonly-used long strings into shorter representations.

"""

import fdb
import fdb.tuple
import random
import os
from directory import directory

fdb.api_version(100)

################
# StringIntern #
################

## State is stored within the given subspace and a local cache of
## frequently-used items is maintained. Use the intern() and lookup()
## functions.


class StringIntern (object):
    STRING_UID = 'S'
    UID_STRING = 'U'
    CACHE_LIMIT_BYTES = 10000000

    def uid_key(self, u): return self.subspace.pack((StringIntern.UID_STRING, u))

    def string_key(self, s): return self.subspace.pack((StringIntern.STRING_UID, s))

    def __init__(self, subspace):
        self.subspace = subspace
        self.uids_in_cache = []
        self.uid_string_cache = {}
        self.string_uid_cache = {}
        self.bytes_cached = 0

    def _evict_cache(self):
        if len(self.uids_in_cache) == 0:
            raise Exception('Cannot evict from empty cache')
        # Random eviction
        i = random.randint(0, len(self.uids_in_cache)-1)

        # remove from uids_in_cache
        u = self.uids_in_cache[i]
        self.uids_in_cache[i] = self.uids_in_cache[len(self.uids_in_cache)-1]
        self.uids_in_cache.pop()

        # remove from caches, account for bytes
        s = self.uid_string_cache[u]
        if s is None:
            raise Exception('Error in cache eviction: string not found')

        del self.uid_string_cache[u]
        del self.string_uid_cache[s]

        size = (len(s) + len(u)) * 2
        self.bytes_cached -= size

    def _add_to_cache(self, s, u):
        while self.bytes_cached > StringIntern.CACHE_LIMIT_BYTES:
            self._evict_cache()

        if u not in self.uid_string_cache:
            self.string_uid_cache[s] = u
            self.uid_string_cache[u] = s
            self.uids_in_cache.append(u)

            size = (len(s) + len(u)) * 2
            self.bytes_cached += size

    @fdb.transactional
    def _find_uid(self, tr):
        tries = 0
        while True:
            u = os.urandom(4+tries)
            if u in self.uid_string_cache:
                continue
            if tr[self.uid_key(u)] == None:
                return u
            tries += 1

    @fdb.transactional
    def _intern_in_db(self, tr, s):
        """
        Look up string s in the intern database and return its normalized
        representation if it already exists. Otherwise, create and record
        the normalized representation before returning it.

        s must fit within a FoundationDB value.
        """
        u = tr[self.string_key(s)]
        if u.present():
            return u
        else:
            newU = self._find_uid(tr)
            tr[self.uid_key(newU)] = s
            tr[self.string_key(s)] = newU
            return newU

    def intern(self, tr, s):
        """
        Look up string s and return its normalized representation, using the
        copy in the cache, if present. Otherwise, intern representation in the
        database and add it to the cache before returning it.

        s must fit within a FoundationDB value.
        """
        if s in self.string_uid_cache:
            return self.string_uid_cache[s]
        u = self._intern_in_db(tr, s)
        self._add_to_cache(s, u)
        return u

    @fdb.transactional
    def lookup(self, tr, u):
        """
        Return the long string associated with the normalized
        representation u.
        """
        if u in self.uid_string_cache:
            return self.uid_string_cache[u]
        s = tr[self.uid_key(u)]
        if s is None:
            raise Exception('String intern identifier not found')
        self._add_to_cache(s, u)
        return s


###################
##    Example    ##
###################


def stringintern_example():
    db = fdb.open()

    location = directory.create_or_open(db, ('tests','stringintern'))
    strs = StringIntern(location)

    def test_insert(tr):
        tr["0"] = strs.intern(tr, "testing 123456789")
        tr["1"] = strs.intern(tr, "dog")
        tr["2"] = strs.intern(tr, "testing 123456789")
        tr["3"] = strs.intern(tr, "cat")
        tr["4"] = strs.intern(tr, "cat")

    test_insert(db)

    tr = db.create_transaction()
    for k,v in tr['0':'9']:
        print k, '=', strs.lookup(tr, v)

if __name__ == "__main__":
    stringintern_example()
