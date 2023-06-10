### To run the crawler/indexer/pagerank


In directory HW9,

Compile crawler/indexer/pagerank

`javac -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:src src/cis5550/jobs/PageRank.java`

Make jar file

`jar cf pagerank.jar src/cis5550/jobs/PageRank.class`

And submit job

`java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank`

While before submitting, in another terminal, run

`java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:src cis5550.kvs.Master 8000`

in another terminal, run

`java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:src cis5550.kvs.Worker 8001 worker1 localhost:8000`

in another terminal, run

`java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:src cis5550.flame.Master 9000 localhost:8000`

in another terminal, run

`java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:src cis5550.flame.Worker 9001 localhost:9000`

to launch kvs master/worker and flame master/worker.