DIME
====

The DIME (Distributed in-memory environment) is a clustered, extremely scalable, in-memory Haskell time series database.

This documentation is just an overview. For a more complete manual, please refer to the actual DIME documentation (coming soon).

Storage
--------
It stores time series data completely in-memory so it is extremely fast. Results are persisted to disk at a
user-defined interval, but data loss is always a possibility (although it should never be too severe). The DIME
is not meant to be used for durable storage but rather as a fast, scalable database that is used mainly to
generate statistics and do analytics, where the loss of a few datapoints in a large time series shouldn't make
too much of a difference. However, the DIME guarantees that you will never get data that you didn't put in.

Additionally, the system performs (currently very basic) load balancing and replication. For example, different
sections of each time series are stored on different nodes. This allows the work of say, differentiating a
time series, to be split up amongst multiple independent workers. This also means that simultaneous writes to
many different time series often end up on different workers, providing greater throughput. However, the DIME
recognizes that some time series are frequently accessed together. For example, a user might want to frequently
do analytics on temperature and humidity data from sensors. The DIME allows users to hint to the runtime that
corresponding parts of these time series should be stored on the same servers, thus reducing the probability
that data will need to be moved when operations require both of these time series.

Queries
--------

The DIME comes with a powerful query language, known as Flow. Flow is a simple, functional language drawing heavily
from Haskell and Miranda. It is a lazy, functional, dynamic language which highly encourages purity of code, but
does not enforce it. It allows quick and easy access to data stored in the DIME, taking care of alignment and
sampling issues on the fly.

For example, to express the per-minute price increase of Apple stock as a ratio of the corresponding increase in
the Dow Jones Industrial average, we can use the Flow code:

    sample @6m (#AAPL:price / #DJIA:score)

If we only wanted the data for the past 5 minutes, we could use the code

    take (@-5m) (sample @6m (#AAPL:price / #DJIA:score))

However, Flow is also Turing-complete, meaning you can perform arbitrarily complex functions on your data. For example,
you can create machine learning algorithms in flow to analyze time series and perform actions based on the results.
In the future, this ability will be learned to enable continuous monitoring of time series (but this might be far off).

Potential Uses
---------------

* Finance (DIME was originally made for my stock monitoring activities)
* Large sensor networks/Internet of Things
* Data warehousing of historical load data, link clicks, etc.

Questions, Bugs, and Comments
------------------------------

Redirect any questions, bugs, comments, and/or complaints to me, Travis Athougies, at travis@athougies.net. Please be
nice, I don't do this for money :)

I hope you enjoy using this software.
