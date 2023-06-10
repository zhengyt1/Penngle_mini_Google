package cis5550.jobs;

import java.util.*;
import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

public class IDF {
  // Define a list of stop words
  private static final List<String> STOP_WORDS = Arrays.asList("a", "an", "and", "are", "as", "at", "be", "but", "by",
          "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their",
          "then", "there", "these", "they", "this", "to", "was", "will", "with");

  public static void run(FlameContext ctx, String[] args) throws Exception {
    KVSClient kvs = ctx.getKVS();
    kvs.persist("IDFRDD");
    ctx.fromTable("job-1.jar-1682971391478.1", s -> newPair(s))
        .flatMapToPair(p -> invertedIndex(p))
        .foldByKey("0", (a, b) -> Integer.toString(Integer.parseInt(a) + Integer.parseInt(b)))
        .saveAsTable("IDF");    
    // ctx.fromTable("plain_text", r -> (r.get("url")) + "," + r.get("description"))
    //    .mapToPair(s -> newPair(s))
    //    .flatMapToPair(p -> invertedIndex(p))
    //    .foldByKey("0", (a, b) -> Integer.toString(Integer.parseInt(a) + Integer.parseInt(b)))
    //    .saveAsTable("IDF");

    Iterator<Row> iter = kvs.scan("IDF");
    while (iter.hasNext()) {
      kvs.putRow("IDFRDD", iter.next());
    }
    Double N = (double)ctx.getKVS().count("IDFRDD");
    iter = kvs.scan("IDFRDD");
    while (iter.hasNext()) {
      Row row = iter.next();
      row.put("acc", ""+ Math.log(N/Double.parseDouble(row.get("acc"))));
      kvs.putRow("IDFRDD", row);
    }
    ctx.output("OK");
  }

  public static String concateFold(String a, String b) {
    return a + (a.isEmpty() ? "" : ",") + b;
  }

  public static FlamePair newPair(String s) {
    return new FlamePair(s.split(",", 2)[0], s.split(",", 2)[1]);
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
      if (STOP_WORDS.contains(word) || word.isEmpty())
        continue;
      posWord.put(word, 1);
      char[] chx = word.toCharArray();
      s.add(chx, chx.length);
      s.stem();
      posWord.put(word, 1);
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
