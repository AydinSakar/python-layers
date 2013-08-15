"""FoundationDB Blob Layer.

Provides the Blob() class for storing potentially large binary objects
in FoundationDB.

"""

import fdb
import fdb.tuple
fdb.api_version(100)

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

########
# Blob #
########

SIZE_KEY = 'S'
ATTRIBUTE_KEY = 'A'
DATA_KEY = 'D'
CHUNK_LARGE = 10000 # all chunks will be not greater than this size
CHUNK_SMALL = 200 # all adjacent chunks will sum to more than this size

class Blob(object):
    """Represents a potentially large binary value in FoundationDB."""

# private functions

    def _internal_storage_key(self):
        return self.subspace.pack( (ATTRIBUTE_KEY,) )

    def _data_key(self, offset):
        return self.subspace.pack( (DATA_KEY, '%16d' % offset) )

    def _data_key_offset(self, key):
        return int(self.subspace.unpack(key)[-1])

    def _size_key(self):
        return self.subspace.pack( (SIZE_KEY,) )

    # returns (key, data, startOffset) or (None, None, None)
    @fdb.transactional
    def _get_chunk_at(self, tr, offset):
        chunkKey = tr.get_key(fdb.KeySelector.last_less_or_equal(self._data_key(offset)))
        if chunkKey is None: # nothing before (sparse)
            return None, None, None
        if chunkKey < self._data_key(0): # off beginning
            return None, None, None
        chunkOffset = self._data_key_offset(chunkKey)
        chunkData = tr[chunkKey]
        if chunkOffset + len(chunkData) <= offset: # in sparse region after chunk
            return None, None, None
        return chunkKey, chunkData, chunkOffset

    def _make_split_point(self, tr, offset):
        key, data, chunkOffset = self._get_chunk_at(tr, offset)
        if key is None: return # already sparse
        if chunkOffset==offset: return # already a split point
        tr[self._data_key(chunkOffset)] = data[:offset-chunkOffset]
        tr[self._data_key(offset)] = data[offset-chunkOffset:]

    @fdb.transactional
    def _make_sparse(self, tr, start, end):
        self._make_split_point(tr, start)
        self._make_split_point(tr, end)
        del tr[self._data_key(start): self._data_key(end)]

    @fdb.transactional
    def _try_remove_split_point(self, tr, offset):
        bKey, bData, bOffset = self._get_chunk_at(tr, offset)
        if bOffset==0 or bKey is None: return False # in sparse region, or at beginning
        aKey, aData, aOffset = self._get_chunk_at(tr, bOffset-1)
        if aKey is None: return False # no previous chunk
        if aOffset+len(aData) != bOffset: return False # chunks can't be joined
        if len(aData)+len(bData) > CHUNK_SMALL: return False # chunks shouldn't be joined
        # yay--merge chunks
        del tr[bKey]
        tr[aKey] = aData+bData
        return True

    @fdb.transactional
    def _write_to_sparse(self, tr, offset, data):
        if not len(data): return
        chunks = (len(data)+CHUNK_LARGE-1) / CHUNK_LARGE
        chunkSize = (len(data)+chunks)/chunks
        chunks = [(n,n+chunkSize) for n in range(0, len(data), chunkSize)]
        for start, end in chunks:
            tr[self._data_key(start+offset)] = data[start:end]

    @fdb.transactional
    def _set_size(self, tr, size):
        tr[self._size_key()] = str(size)

## public functions below            

    def __init__(self, subspace):
        """
        Create a new object representing a binary large object (blob).

        Only keys within the subspace will be used by the
        object. Other clients of the database should refrain from
        modifying the subspace.
        """
        self.subspace = subspace

    @fdb.transactional
    def delete(self, tr):
        """Delete all key-value pairs associated with the blob."""
        del tr[self.subspace.range()]

    @fdb.transactional
    def get_size(self, tr):
        """Get the size of the blob."""
        try:
            return int(tr[self._size_key()])
        except (ValueError, TypeError):
            return 0

    @fdb.transactional
    def read(self, tr, offset, n):
        """
        Read from the blob, starting at offset, retrieving up to n
        bytes (fewer then n bytes are returned when the end of the
        blob is reached).
        """
        chunks = tr.get_range(
            fdb.KeySelector.last_less_or_equal(self._data_key(offset)),
            fdb.KeySelector.first_greater_or_equal(self._data_key(offset + n)))
        size = self.get_size(tr)
        if offset >= size:
            return ""
        result = bytearray(min(n, size - offset))
        for chunkKey, chunkData in chunks:
            chunkOffset = self._data_key_offset(chunkKey)
            for i in range(len(chunkData)):
                rPos = chunkOffset+i-offset
                if rPos>=0 and rPos<len(result):
                    result[rPos] = chunkData[i]
        return str(result)

    @fdb.transactional
    def write(self, tr, offset, data):
        """
        Write data to the blob, starting at offset and overwriting any
        existing data at that location. The length of the blob is
        increased if necessary.
        """
        if not len(data): return
        end = offset+len(data)
        self._make_sparse(tr, offset, end)
        self._write_to_sparse(tr, offset, data)
        self._try_remove_split_point(tr, offset)
        oldLength = self.get_size(tr)
        if end > oldLength:
            self._set_size(tr, end) # lengthen file if necessary
        else:
            self._try_remove_split_point(tr, end) # write end needs to be merged

    @fdb.transactional
    def append(self, tr, data):
        """Append the contents of data onto the end of the blob."""
        if not len(data): return
        oldLength = self.get_size(tr)
        self._write_to_sparse(tr, oldLength, data)
        self._try_remove_split_point(tr, oldLength)
        tr[self._size_key()] = str(oldLength + len(data))

    @fdb.transactional
    def truncate(self, tr, new_length):
        """
        Change the blob length to new_length, erasing any data when
        shrinking, and filling new bytes with 0 when growing.
        """
        self._make_sparse(tr, new_length, int(tr[self._size_key()]))
        tr[self._size_key()] = str(new_length)

###################
##    Example    ##
###################        

@fdb.transactional
def print_blob(tr, b):
    s = b.get_size(tr)
    print "blob is", s, "bytes:"
    print b.read(tr, 0, s)

def test_blob():
    import time

    db = fdb.open()

    location = Subspace(('testblob',))

    b = Blob(location)

    print "deleting old"
    b.delete(db)

    print "writing"
    b.append(db, 'asdf')
    b.append(db, 'jkl;')
    b.append(db, 'foo')
    b.append(db, 'bar')

    print_blob(db, b)

    big_data = 1000000
    print "writing lots of data"
    for i in range(50):
        print ".",
        b.append(db, '.'*100000)

    print ""
    print "reading section of large blob..."
    t = time.time()
    s = len(b.read(db, 1234567, big_data))
    assert s == big_data
    print "got big section of blob"

if __name__ == "__main__":
    test_blob()
