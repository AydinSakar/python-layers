python-layers
=============

What's here
-----------

Example layers written in Python for [FoundationDB](http://foundationdb.com/). These layers require FoundationDB [Beta 1](http://foundationdb.com/get/) to work.

What are "layers"
-----------------

FoundationDB decouples its data storage technology from its data model. Layers add capabilities to FoundationDB's ordered key-value API. A layer can provide a new data model, compatibility with existing systems, or even serve as an entire framework.

These layers demonstrate the concept of layer development: that even simple layers inherit the transactional, scalable, fault-tolerant properties of FoundationDB. While functional, these layers are designed primarily to be simple, clear examples.

The layers:
-----------

 * **blob.py** - Arbitrary-sized and sparse large binary objects.
 * **counter.py** - A high performance counter that uses advanced techniques to dynamically shard itself under high contention conditions.
 * **spatial.py** - A spatial index for 2D points that allows efficient queries of axis-aligned rectangular regions. Does dimensionality reduction via a Z-order fractal curve (aka geohash).
 * **stringintern.py** - For interning (aka normalizing, aliasing) commonly-used long strings into shorter representations. Maintains the normalization state in the database, as well as a local cache for high performance.

Next steps
----------

Discussion of layers and layer development takes place on the [FoundationDB community site](http://community.foundationdb.com/). (Sign up [here](http://foundationdb.com/).)
