package cis5550.jobs;

import java.util.*;
import cis5550.flame.*;

public class Indexer {

  public static void run(FlameContext ctx, String[] args) throws Exception {
    FlamePairRDD pairRDD = ctx.fromTable("crawl", r -> r.get("url") + "," + r.get("page")).mapToPair(s -> newPair(s));
    
    FlamePairRDD tmp = pairRDD.flatMapToPair(p -> invertedIndex(p)).foldByKey("0", (a, b) -> {
      return Integer.toString(Integer.parseInt(a) + Integer.parseInt(b));
    });

    tmp.saveAsTable("tmp");

    // FlamePairRDD indexRDD = pairRDD.flatMapToPair(p -> invertedIndex(p)).foldByKey("", (a, b) -> concateFold(a, b))
    //     .foldByKey("", (a, feq) -> sortByFeq(a, feq));
    
    // indexRDD.saveAsTable("index");
    ctx.output("OK");
  }

  public static String concateFold(String a, String b) {
    return a + (a.isEmpty() ? "" : ",") + b;
  }

  public static FlamePair newPair(String s) {
    return new FlamePair(s.split(",", 2)[0], s.split(",", 2)[1]);
  }

  public static String sortByFeq(String a, String feq) {
    String[] urlElements = feq.split(",");
    Arrays.sort(urlElements, (e1, e2) -> {
      String[] splitedE1 = e1.split(":");
      String[] splitedE2 = e2.split(":");
      int n1 = splitedE1[splitedE1.length - 1].split("\\s+").length;
      int n2 = splitedE2[splitedE2.length - 1].split("\\s+").length;
      return Integer.compare(n2, n1);
    });

    String sortedUrls = String.join(",", urlElements);
    return a + sortedUrls;
  }

  /**
   * Builds an inverted index for the given FlamePair object.
   *
   * @param p The FlamePair object for which the inverted index is built.
   * @return A LinkedList of FlamePair objects representing the inverted index.
   */
  public static LinkedList<FlamePair> invertedIndex(FlamePair p) {
    // Extract the necessary information from the FlamePair object
    String url = p._1();
    String html = p._2().replaceAll("\\<.*?\\>", " ").replaceAll("\\p{Punct}", " ").replaceAll("\\s+", " ")
        .toLowerCase().trim();
    String[] words = html.split("\\s+"); // split the string into words

    LinkedList<FlamePair> list = invertHelper(url, words);

    // Return the inverted index
    return list;
  }


  /**
   * Helper method to build the inverted index for a given list of words.
   *
   * @param url The URL of the webpage from which the words were extracted.
   * @param words An array of words extracted from the webpage.
   * @return The updated LinkedList of FlamePair objects representing the inverted index.
   */
  public static LinkedList<FlamePair> invertHelper(String url, String[] words) {
    // Build the inverted index using the helper method
    LinkedList<FlamePair> list = new LinkedList<FlamePair>();

    // Create a Stemmer object to stem the words
    Stemmer s = new Stemmer();

    // Create a HashMap to store the position of each word in the webpage
    HashMap<String, Integer> posWord = new HashMap<>();

    // Iterate over the words in the array and add them to the HashMap
    for (int i = 0; i < words.length; i++) {
        String word = words[i];
        if (!word.isEmpty()) {
            Integer cur = posWord.getOrDefault(word, 0);
            posWord.put(word, ++cur);
            char[] chx = word.toCharArray();
            s.add(chx, chx.length);
            s.stem();
            if (!s.toString().equals(word)) 
              posWord.put(word, ++cur);
        }
    }

    // Convert the HashMap into a LinkedList of FlamePair objects and add them to the list
    for (Map.Entry<String, Integer> entry : posWord.entrySet())
        list.add(new FlamePair(entry.getKey(), Integer.toString(entry.getValue())));

    // Return the updated LinkedList of FlamePair objects representing the inverted index
    return list;
  }

}

// javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src src/cis5550/jobs/Indexer.java
// jar -cvf indexer.jar src/cis5550/jobs/Indexer.class
// java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer


// java -cp lib/kvs.jar:lib/webserver.jar:src:lib/flame.jar cis5550.flame.Master 9000 localhost:8000
// java -cp lib/kvs.jar:lib/webserver.jar:src:lib/flame.jar cis5550.flame.Worker 9001 localhost:9000

// java -cp lib/kvs.jar:lib/webserver.jar:src cis5550.kvs.Master 8000
// java -cp lib/kvs.jar:lib/webserver.jar:src cis5550.kvs.Worker 8001 worker1 localhost:8000

// java -cp lib/kvs.jar:lib/webserver.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Master 8000
// java -cp lib/kvs.jar:lib/webserver.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Worker 8001 worker1 localhost:8000
// java -cp lib/kvs.jar:lib/webserver.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Worker 8002 worker2 localhost:8000
// java -cp lib/kvs.jar:lib/webserver.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Worker 8003 worker3 localhost:8000
// java -cp lib/kvs.jar:lib/webserver.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Worker 8004 worker4 localhost:8000
// java -cp lib/kvs.jar:lib/webserver.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Worker 8005 worker5 localhost:8000
// java -cp lib/kvs.jar:lib/webserver.jar:lib/jsoup-1.15.4.jar:src cis5550.kvs.Worker 8006 worker6 localhost:8000
