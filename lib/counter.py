"""FoundationDB High Contention Counter.

Provides the Counter class, which represents an integer value in the
database which can be incremented, added to, or subtracted from within
a transaction without conflict.

"""

import fdb
import fdb.tuple
import random
import os

fdb.api_version(21)

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

###########
# Counter #
###########

def _encode_int(i):
    return fdb.tuple.pack((i,)) # use the tuple layer to pack integers

def _decode_int(s):
    return fdb.tuple.unpack(s)[0]

def randID():
    return os.urandom(20) # this relies on good random data from the OS to avoid collisions

class Counter:
    """Represents an integer value which can be incremented without conflict.

    Uses a sharded representation (which scales with contention) along
    with background coalescing.

    """

    def __init__(self, db, subspace):
        self.subspace = subspace
        self.db = db
        
    def _coalesce(self, N):
        total = 0
        tr = self.db.create_transaction()
        try:
        
            # read N writes from a random place in ID space
            loc = self.subspace.pack((randID(),))
            if random.random() < 0.5:
                shards = tr.snapshot.get_range(loc, self.subspace.range().stop, limit=N);
            else:
                shards = tr.snapshot.get_range(self.subspace.range().start, loc, limit=N, reverse = True);

            # remove read shards transaction
            for k,v in shards:
                total += _decode_int(v)
                tr[k] # real read for isolation
                del tr[k]
                
            tr[self.subspace.pack((randID(),))] = _encode_int(total)
                
            ## note: no .wait() on the commit below--this just goes off
            ## into the ether and hopefully sometimes works :)
            ##
            ## the hold() function saves the tr variable so that the transaction
            ## doesn't get cancelled as tr goes out of scope!
            c = tr.commit()
            def hold(_,tr=tr): pass
            c.on_ready(hold)
            
        except fdb.FDBError as e:
            pass

    @fdb.transactional
    def get_transactional(self, tr):
        """Get the value of the counter.
        
        Not recommended for use with read/write transactions when the counter
        is being frequently updated (conflicts will be very likely).
        """
        total = 0
        for k,v in tr[self.subspace.range()]:
            total += _decode_int(v)
        return total
    
    @fdb.transactional
    def get_snapshot(self, tr):
        """
        Get the value of the counter with snapshot isolation (no
        transaction conflicts).
        """
        total = 0
        for k,v in tr.snapshot[self.subspace.range()]:
            total += _decode_int(v)
        return total
    
    @fdb.transactional
    def add(self, tr, x):
        """Add the value x to the counter."""

        tr[self.subspace.pack((randID(),))] = _encode_int(x)
        
        # Sometimes, coalesce the counter shards
        if random.random() < 0.1:
            self._coalesce(20)

    ## sets the counter to the value x
    @fdb.transactional
    def set_total(self, tr, x):
        """Set the counter to value x."""
        value = self.get_snapshot(tr)
        self.add(tr, x - value)

##################
# simple example #
##################

def counter_example_1(db, location):
    
    location = Subspace( ('bigcounter',) )
    c = Counter(db, location)
    
    for i in range(500):
        c.add(db, 1)
    print c.get_snapshot(db) #500

#####################################
# high-contention, threaded example #
#####################################

def incrementer_thread(counter, db, n):
    for i in range(n):
        counter.add(db, 1)

def counter_example_2(db, location):
    import threading
    
    c = Counter(db, location)

    ## 50 incrementer_threads, each doing 10 increments
    threads = [
        threading.Thread(target=incrementer_thread, args=(c, db, 10))
        for i in range(50)]
    for thr in threads: thr.start()
    for thr in threads: thr.join()

    print c.get_snapshot(db) #500

if __name__ == "__main__":
    db = fdb.open()
    location = Subspace( ('bigcounter',) )
    del db[location.range()]
    
    print "doing 500 inserts in 50 threads"
    counter_example_2(db, location)
    print len(db[:]), "counter shards remain in database"

    print "doing 500 inserts in one thread"
    counter_example_1(db, location)
    print len(db[:]), "counter shards remain in database"
