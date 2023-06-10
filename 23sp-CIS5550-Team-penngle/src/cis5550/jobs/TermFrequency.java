package cis5550.jobs;

import java.util.*;
import java.io.*;
import cis5550.flame.*;
import cis5550.kvs.KVS;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class TermFrequency {
  // Define a list of stop words
  private static final List<String> STOP_WORDS = Arrays.asList("a", "an", "and", "are", "as", "at", "be", "but", "by",
      "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their",
      "then", "there", "these", "they", "this", "to", "was", "will", "with");

  private static final Set<String> commonWrods = getCommonEnglSet();

  public static void run(FlameContext ctx, String[] args) throws Exception {
    KVSClient kvs = ctx.getKVS();
    FlameRDD fromTable = ctx.fromTable("plain_text", r -> {
      String context = r.get("text").replaceAll("[^a-zA-Z]", " ");
      int start = Math.min(1388, context.length());
      if (start == context.length()) {
        start = 0;
      }
      int end = Math.min(5000, context.length());

      context = context.substring(start, end).toLowerCase();
      return Hasher.hash(r.get("url")) + "," + context;
    });
    FlamePairRDD baseRDD = fromTable.mapToPair(s -> newPair(s));
    fromTable.saveAsTable("fromTable");
    kvs.delete("fromTable");

    // FlamePairRDD invertedRDD = baseRDD.flatMapToPair(p -> invertedIndex(p));
    // FlamePairRDD freqRDD = invertedRDD.foldByKey("", (a, b) -> concateFold(a,
    // b));
    // kvs.persist("freqRDD");
    // freqRDD.saveAsTable("freq");
    // Iterator<Row> iter = kvs.scan("freq");
    // while (iter.hasNext()) {
    // kvs.putRow("freqRDD", iter.next());
    // }

    // Double N = (double) kvs.count("freqRDD");
    // kvs.delete("freq");
    // System.gc();

    // FlamePairRDD idfRDD = invertedRDD.foldByKey("0", (a, b) -> _sum(a,
    // b)).foldByKey("", (a, b) -> _divide(a, b, N));
    // kvs.persist("idfRDD");
    // idfRDD.saveAsTable("idf");
    // iter = kvs.scan("idf");
    // while (iter.hasNext()) {
    // kvs.putRow("idfRDD", iter.next());
    // }
    // kvs.delete("idf");
    // System.gc();

    // FlamePairRDD wij = baseRDD.flatMapToPair(p -> wij(p, ctx)).foldByKey("", (a,
    // b) -> concateFold(a, b));
    // kvs.persist("wijRDD");
    // wij.saveAsTable("wij");
    // Iterator<Row> iter = kvs.scan("wij");
    // while (iter.hasNext()) {
    // kvs.putRow("wijRDD", iter.next());
    // }

    Iterator<Row> iter = kvs.scan("wijRDD");
    kvs.persist("wijCol");
    while (iter.hasNext()) {
      Row row = iter.next();
      String word = row.key();
      String[] col = row.get("acc").split(",");

      Row result = new Row(word);
      for (String s : col) {
        String[] pair = s.split("\\s+", 2);
        String url = pair[0];
        String freq = pair[1];
        result.put(url, freq);
      }
      kvs.putRow("wijCol", result);
    }

    ctx.output("OK");
  }

  public static LinkedList<FlamePair> wij(FlamePair p, FlameContext ctx) {
    try {
      // Extract the necessary information from the FlamePair object
      String url = p._1();
      String html = p._2().replaceAll("\\s+", " ").toLowerCase().trim();
      String[] words = html.split("\\s+"); // split the string into words
      // Return the inverted index
      LinkedList<FlamePair> list = new LinkedList<FlamePair>();

      // Create a Stemmer object to stem the words
      Stemmer s = new Stemmer();

      // Create a HashMap to store the position of each word in the webpage
      HashMap<String, Double> feqWord = new HashMap<>();
      KVSClient kvs = ctx.getKVS();

      // Iterate over the words in the array and add them to the HashMap
      Double N = 0.0;
      for (int i = 0; i < words.length; i++) {
        String word = words[i];
        if (STOP_WORDS.contains(word) || word.isEmpty() || word.length() >= 18 || word.length() <= 2)
          continue;
        char[] chx = word.toCharArray();
        s.add(chx, chx.length);
        s.stem();
        String stemmedWord = s.toString();

        if (STOP_WORDS.contains(stemmedWord) || !commonWrods.contains(stemmedWord) || feqWord.containsKey(
            stemmedWord)) {
          continue;
        }
        byte[] freq_b = kvs.get("freqRDD", stemmedWord, "acc");
        byte[] idf_b = kvs.get("idfRDD", stemmedWord, "acc");
        if (freq_b != null && idf_b != null) {
          Double freq = 0.0;
          String[] freq_s = new String(freq_b).split(",");
          for (String x : freq_s) {
            if (x.startsWith(url)) {
              freq = Double.parseDouble(x.split("\\s+", 2)[1]);
            }
          }
          Double idf = Double.parseDouble(new String(idf_b));
          feqWord.put(stemmedWord, (freq * idf));
        }
      }

      // Convert the HashMap into a LinkedList of FlamePair objects and add them to
      // the list
      Double D = 0.0;
      for (Map.Entry<String, Double> entry : feqWord.entrySet()) {
        Double w = entry.getValue();
        list.add(new FlamePair(entry.getKey(), url + " " + w));
        D += w * w;
      }

      list.add(new FlamePair("(D)", url + " " + Math.sqrt(D)));
      // Return the updated LinkedList of FlamePair objects representing the inverted
      // index
      return list;
    } catch (Exception e) {
      return new LinkedList<FlamePair>();
    }
  }

  public static Set<String> getCommonEnglSet() {
    Set<String> wordsSet = new HashSet<>();
    Stemmer s = new Stemmer();
    try {
      // File file = new File("words.txt");
      File file = new File("wordlist.10000");
      Scanner scanner = new Scanner(file);
      while (scanner.hasNextLine()) {
        String word = scanner.nextLine().replaceAll("[^a-zA-Z]", "").replaceAll("\\s+", "").toLowerCase();
        char[] chx = word.toCharArray();
        s.add(chx, chx.length);
        s.stem();
        String stemmedWord = s.toString();
        wordsSet.add(stemmedWord);
      }
      scanner.close();
    } catch (Exception e) {
      System.out.println("File not found.");
    }
    System.out.println("Set size + " + wordsSet.size());
    return wordsSet;
  }

  public static LinkedList<FlamePair> transpose(FlamePair p) {
    LinkedList<FlamePair> list = new LinkedList<FlamePair>();
    String[] wordElements = p._2().split(",");
    for (String wordElement : wordElements) {
      String[] splitedWordElement = wordElement.split("\\s+", 2);
      String url = splitedWordElement[0];
      String freq = splitedWordElement[1];
      list.add(new FlamePair(url, freq));
    }
    return list;
  }

  public static String _sum(String a, String b) {
    return "" + (Double.parseDouble(a) + 1.0);
  }

  public static String _divide(String a, String b, Double N) {
    if (Double.parseDouble(b) > 0.0) {
      return "" + Math.log(N / Double.parseDouble(b));
    }
    return "0";
  }

  public static String _max(String a, String b) {
    return "" + Math.max(Double.parseDouble(a), Double.parseDouble(b));
  }

  public static String concateFold(String a, String b) {
    return a + (a.isEmpty() ? "" : ",") + b;
  }

  public static FlamePair newPair(String s) {
    try {
      String url = s.split(",", 2)[0];
      String page = s.split(",", 2)[1];
      return new FlamePair(url, page);
    } catch (Exception e) {
      return new FlamePair(s, "");
    }
  }

  /**
   * Builds an inverted index for the given FlamePair object.
   *
   * @param p The FlamePair object for which the inverted index is built.
   * @return A LinkedList of FlamePair objects representing the inverted index.
   */
  public static LinkedList<FlamePair> invertedIndex(FlamePair p) {
    try {
      // Extract the necessary information from the FlamePair object
      String url = p._1();
      String html = p._2().replaceAll("\\s+", " ").toLowerCase().trim();
      String[] words = html.split("\\s+"); // split the string into words
      LinkedList<FlamePair> list = invertHelper(url, words);
      // Return the inverted index
      return list;
    } catch (Exception e) {
      return new LinkedList<FlamePair>();
    }
  }

  /**
   * Helper method to build the inverted index for a given list of words.
   *
   * @param url   The URL of the webpage from which the words were extracted.
   * @param words An array of words extracted from the webpage.
   * @param list  The LinkedList of FlamePair objects representing the inverted
   *              index.
   * @return The updated LinkedList of FlamePair objects representing the inverted
   *         index.
   */
  public static LinkedList<FlamePair> invertHelper(String url, String[] words) {
    // Build the inverted index using the helper method
    LinkedList<FlamePair> list = new LinkedList<FlamePair>();

    // Create a Stemmer object to stem the words
    Stemmer s = new Stemmer();

    // Create a HashMap to store the position of each word in the webpage
    HashMap<String, Integer> feqWord = new HashMap<>();

    // Iterate over the words in the array and add them to the HashMap
    Double N = 0.0;
    for (int i = 0; i < words.length; i++) {
      String word = words[i];
      if (STOP_WORDS.contains(word) || word.isEmpty() || word.length() >= 18 || word.length() <= 2)
        continue;
      // feqWord.put(word, feqWord.getOrDefault(word, 0) + 1);
      char[] chx = word.toCharArray();
      s.add(chx, chx.length);
      s.stem();
      String stemmedWord = s.toString();

      if (STOP_WORDS.contains(stemmedWord) || !commonWrods.contains(stemmedWord)) {
        continue;
      }

      feqWord.put(stemmedWord, feqWord.getOrDefault(stemmedWord, 0) + 1);
    }

    Double largest = Double.MIN_VALUE;
    Double alpha = 0.4;
    for (Integer value : feqWord.values()) {
      if (value > largest) {
        largest = (double) value;
      }
    }

    // Convert the HashMap into a LinkedList of FlamePair objects and add them to
    // the list
    for (Map.Entry<String, Integer> entry : feqWord.entrySet())
      list.add(new FlamePair(entry.getKey(), url + " " + (alpha + (1 - alpha) * entry.getValue() / largest)));

    // Return the updated LinkedList of FlamePair objects representing the inverted
    // index
    return list;
  }

}

// javac -cp lib/webserver.jar:lib/kvs.jar:lib/flame.jar --source-path src
// src/cis5550/jobs/Indexer.java
// jar -cvf indexer.jar src/cis5550/jobs/Indexer.class
// java -cp lib/kvs.jar:lib/webserver.jar:lib/flame.jar
// cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer

// java -cp lib/kvs.jar:lib/webserver.jar:src:lib/flame.jar cis5550.flame.Master
// 9000 localhost:8000
// java -cp lib/kvs.jar:lib/webserver.jar:src:lib/flame.jar cis5550.flame.Worker
// 9001 localhost:9000

// java -cp lib/kvs.jar:lib/webserver.jar:src cis5550.kvs.Master 8000
// java -cp lib/kvs.jar:lib/webserver.jar:src cis5550.kvs.Worker 8001 worker1
// localhost:8000

// java -cp lib/webserver.jar:lib/kvs.jar cis5550.kvs.KVSClient localhost:8000
// get freqRDD 18 http://simple.crawltest.cis5550.net:80/