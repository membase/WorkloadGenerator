This java workload generator is designed to put a random, but significant
workload on a "memcapable" server.  By default it will take a "pool" and
"bucket" as parameters, then run a one minute workload with four threads
randomly doing adds/sets/gets/prepends/appends/deleten on random intervals
between 100ms and 1sec.

It's intended to be easy to read code, so the MemcapableWorker Class is a
simple, readable class with the methods implemented there directly.