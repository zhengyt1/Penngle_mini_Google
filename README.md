# Components for a search engine

A web server (HW1–3), a distributed key-value store (HW4+5), a spark-like analytics engine (HW6+7), a crawler (HW8), and a simple indexer and PageRank (HW9).

# Final Project -- Penngle

See details in `23sp-CIS5550-Team-penngle` folder.

## Crawler

### Data We crawled

Our web crawler focuses on extracting data from BBC news and wiki pages and generates tables of URLs and plain text web content for use in our search engine. However, we have encountered several challenges during the crawling process, such as broken or unresponsive pages, limitations on available memory, and issues with performance. In order to address these challenges, we have implemented several enhancements to our crawler. And finally, we
crawl one million documents in this stage.

- We fine-tuned the find urls function and improved our normalization techniques to
make them more robust.
- We set a timeout to discard low-responsive pages and prevent the crawler from wasting
time on unproductive targets.
- To make the crawler restartable and easy to monitor, we saved the frontier at the end
of each iteration and constantly monitored its progress.
- We kept a record of the crawled URLs to avoid infinite redirects and improve perfor-
mance while minimizing additional key-value store read/write operations.
- To accelerate the crawler, we made as few HTTP requests as possible and optimized
our network traffic.
- Finally, we manually monitored the crawler and deployed it to Amazon EC2 for better
scalability and reliability.

![7241683138507_ pic](https://user-images.githubusercontent.com/112508286/236013481-74b12003-f96e-425e-9280-135fd5d6bb65.jpg)
![crawled_data](https://user-images.githubusercontent.com/112508286/236362698-1af5a4dc-dea3-4408-ba9c-a32d5a2e79bb.png)


### Run our crawler

Compile crawler

`javac -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:lib/jsoup-1.15.4.jar:src src/cis5550/jobs/Crawler.java`

Make jar file

`jar cf crawler.jar src/cis5550/jobs/Crawler.class`

And submit job, if it's your first crawl or you want crawl something rather than previous frontier pages, run

`java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:lib/jsoup-1.15.4.jar:src cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler seed [your seed url(s)]`

(if you still want to continue crawling from last break, replace `seed` with anything else.)

While ***before*** submitting, in another terminal, start KVS Master, run

`java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Master 8000`

in another terminal, start KVS Woker(s), run

`java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Worker 8001 worker1 localhost:8000`

in another terminal, start Flame Masteer, run

`java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:lib/jsoup-1.15.4.jar:src cis5550.flame.Master 9000 localhost:8000`

in another terminal, start Flame Worker(s), run

`java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar:lib/jsoup-1.15.4.jar:src cis5550.flame.Worker 9001 localhost:9000`

**Other parts can be compile/run in the same way, refer to our Makefile for more details**

## Third-party components we used
- Jsoup

## Some search examples
![bbc](https://user-images.githubusercontent.com/112508286/236362735-6a2b8700-8d16-4db5-a194-4ddea0bb1279.png)
![china](https://user-images.githubusercontent.com/112508286/236362736-aa94e9d9-dbd9-492a-b55a-4680079a4798.png)
![culture_difference](https://user-images.githubusercontent.com/112508286/236362738-a31d1527-fd38-44bc-99d9-fda37a1e5cad.png)
![culture](https://user-images.githubusercontent.com/112508286/236362740-87cdb256-629b-40a0-8888-7b92cb8fb790.png)
