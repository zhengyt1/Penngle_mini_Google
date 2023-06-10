all: run_kvs run_flame

tf:
	javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src src/cis5550/jobs/TermFrequency.java
	jar -cvf termFrequency.jar src/cis5550/jobs/TermFrequency.class
	nohup java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 termFrequency.jar cis5550.jobs.TermFrequency &

try:
	javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src src/cis5550/jobs/Try.java
	jar -cvf Try.jar src/cis5550/jobs/Try.class
	java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 Try.jar cis5550.jobs.Try

idf:
	javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src src/cis5550/jobs/IDF.java
	jar -cvf idf.jar src/cis5550/jobs/IDF.class
	java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 idf.jar cis5550.jobs.IDF

page:
	javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src src/cis5550/jobs/PageRank.java
	jar -cvf pagerank.jar src/cis5550/jobs/PageRank.class
	java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.01

pagep:
	javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src src/cis5550/jobs/PageRank.java
	jar -cvf pagerank.jar src/cis5550/jobs/PageRank.class
	java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.01 50

server:
	javac -cp lib/webserver.jar:lib/kvs.jar:src --source-path src src/cis5550/jobs/Master.java
	sudo nohup java -cp lib/webserver.jar:lib/kvs.jar:src src/cis5550/jobs/Master.java 80 &

test: 
	javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src src/cis5550/jobs/Query.java 
	java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:src cis5550.jobs.Query

run_kvs:
	nohup java -cp lib/kvs.jar:lib/webserver.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Master 8000 &
	nohup java -Xmx6g -cp lib/kvs.jar:lib/webserver.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Worker 8001 data localhost:8000 &

run_flame:
	nohup java -cp lib/kvs.jar:lib/webserver.jar:src:lib/flame.jar cis5550.flame.Master 9000 localhost:8000 &
	nohup java -cp lib/kvs.jar:lib/webserver.jar:src:lib/flame.jar cis5550.flame.Worker 9001 localhost:9000 &
	
# plain_text is large   plain_text1 is small
# plain_text2 is large  plain_text is small
small:
	mv worker1/plain_text.table worker1/plain_text2.table 
	mv worker1/plain_text1.table worker1/plain_text.table 

# plain_text is small  plain_text2 is large
# plain_text1 is small plain_text is large
large: 
	mv worker1/plain_text.table worker1/plain_text1.table 
	mv worker1/plain_text2.table worker1/plain_text.table 

kill:
	ps aux | grep "java.*kvs" | awk '{print $2}' | xargs kill

show:
	ps aux | grep "java.*kvs"

clean:
	rm -f *.jar
	rm -f src/cis5550/*/*.class
# rm -f worker*/concatedRDD.table worker*/invertedIndex.table worker*/pairRDD.table worker*/tfRDD.table worker*/freqRDD.table worker*/idfRDD.table worker*/DRDD.table worker*/wijRDD.table
