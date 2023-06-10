package cis5550.jobs;

import cis5550.kvs.KVS;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class Query {
  private static final List<String> STOP_WORDS = Arrays.asList("a", "an", "and", "are", "as", "at", "be", "but", "by",
      "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their",
      "then", "there", "these", "they", "this", "to", "was", "will", "with");
  private static final Set<String> commonWrods = getCommonEnglSet();
  public static final HashMap<String, HashMap<String, Double>> wijRDD = getTfidf(); // word, url ,val
  public static final HashMap<String, Double> idfRDD = getIdf(); // word ,val
  public static final ArrayList<String> allDocuments = getAllDocuments();

  // public static void main(String args[]) throws Exception {
  //   System.out.println(queryThings("it is important to have culture difference"));
  // }

  public static ArrayList<String> getAllDocuments(){
    System.out.println("starts initializing all documents");
    KVSClient kvs = new KVSClient("localhost:8000");
    ArrayList<String> documents = new ArrayList<>();
    Iterator<Row> iter;
    try {
      iter = kvs.scan("pagerank3");
    } catch (Exception e) {
      return documents;
    }
    while (iter.hasNext()) {
      Row row = iter.next();
      String url = row.key();
      if (url != null) {
        documents.add(url);
      }
    }
    System.out.println("finished initializing all documents");
    return documents;
  }

  public static HashMap<String, Double> getIdf() {
    System.out.println("starts initializing all getifd");
    HashMap<String, Double> idfHash = new HashMap<>();

    KVSClient kvs = new KVSClient("localhost:8000");
    Iterator<Row> iter;
    try {
      iter = kvs.scan("idfRDD");
    } catch (Exception e) {
      return idfHash;
    }
    while (iter.hasNext()) {
      Row row = iter.next();
      String word = row.key();
      Double freq = Double.parseDouble(row.get("acc"));
      idfHash.put(word, freq);
    }
    System.out.println("idfRDD is done");
    return idfHash;
  }

  public static HashMap<String, HashMap<String, Double>> getTfidf() {
    System.out.println("starts initializing all getTfidf");
    HashMap<String, HashMap<String, Double>> tfidf = new HashMap<>();

    KVSClient kvs = new KVSClient("localhost:8000");
    Iterator<Row> iter;
    try {
      iter = kvs.scan("wijRDD");
    } catch (Exception e) {
      return tfidf;
    }
    while (iter.hasNext()) {
      Row row = iter.next();
      String word = row.key();
      HashMap<String, Double> tfidfMap = new HashMap<>();
      String[] urlFreq = row.get("acc").split(",");
      for (String urlFreqPair : urlFreq) {
        String[] pair = urlFreqPair.split("\\s+", 2);
        String url = pair[0];
        Double freq = Double.parseDouble(pair[1]);
        tfidfMap.put(url, freq);
      }
      tfidf.put(word, tfidfMap);
    }
    System.out.println("getTfidf done");
    return tfidf;
  }

  public static String queryThings(String query) throws Exception {
    KVSClient kvs = new KVSClient("localhost:8000");

    ArrayList<String> parsedQuery = parseQuery(query);

    Double Q = 0.0;
    HashMap<String, Double> WIQ = new HashMap<>();

    for (String term : parsedQuery) {
      Double wiq = wiq(kvs, 0.4, term, parsedQuery);
      WIQ.put(term, wiq);
      Q += wiq * wiq;
    }

    PriorityQueue<DocumentScore> maxHeap = new PriorityQueue<>(3);

    for (String document : allDocuments){
      Double score = score(kvs, document, parsedQuery, WIQ, Q);
      DocumentScore documentScore = new DocumentScore(document, score);
      if (score.isNaN() || score == 0.0) {
        continue;
      }
      if (maxHeap.size() < 21) {
        maxHeap.add(documentScore);
      } else if (score > maxHeap.peek().getScore()) {
        maxHeap.poll();
        maxHeap.add(documentScore);
      }
    }

    List<DocumentScore> top3Documents = new ArrayList<>();
    while (!maxHeap.isEmpty()) {
      top3Documents.add(maxHeap.poll());
    }

    List<String> docstr = new ArrayList<>();
    System.out.println("******************************");
    System.out.println(query);
    for (DocumentScore document : top3Documents) {
      docstr.add(document.document);
      System.out.println(document);
    }
    System.out.println("******************************");
    return toJsonArray(docstr);
  }

  private static String toJsonArray(List<String> urls) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    boolean first = true;
    for (String url : urls) {
      if (!first) {
        sb.append(",");
      }
      sb.append("{\"url\":\"").append(url).append("\"}");
      first = false;
    }
    sb.append("]");
    return sb.toString();
  }

  private static class DocumentScore implements Comparable<DocumentScore> {
    private final String document;
    private final double score;

    public DocumentScore(String document, double score) {
      this.document = document;
      this.score = score;
    }

    public String getDocument() {
      return document;
    }

    public double getScore() {
      return score;
    }

    @Override
    public String toString() {
      return document + " " + score;
    }

    @Override
    public int compareTo(DocumentScore other) {
      return Double.compare(other.score, score); // Compare in reverse order for max heap
    }
  }

  public static ArrayList<String> parseQuery(String query) {
    Stemmer s = new Stemmer();
    String cleanedQuery = query.replaceAll("[^a-zA-Z]", " ");
    List<String> terms = Arrays.asList(cleanedQuery.split("\\s+"));
    ArrayList<String> parsedQuery = new ArrayList<>();

    for (String term : terms) {
      char[] chx = term.toLowerCase().trim().toCharArray();
      s.add(chx, chx.length);
      s.stem();
      String stemmedWord = s.toString();
      if (STOP_WORDS.contains(stemmedWord) || stemmedWord.isEmpty() ||
          !commonWrods.contains(stemmedWord))
        continue;
      parsedQuery.add(stemmedWord);
    }
    return parsedQuery;
  }

  public static double score(KVSClient kvs, String document, ArrayList<String> query, HashMap<String, Double> WIQ,
      Double Q) throws IOException {
    Double D = 0.0;
    try {
      D = wijRDD.get("(D)").get(document);
      if (D == null) {
        return 0.0;
      }
    } catch (Exception e) {
      try {
        byte[] wij_b = kvs.get("(D)", "acc", document);
        if (wij_b == null) {
          return 0.0;
        }
        Double wij = Double.parseDouble(new String(wij_b));
        return wij;
      } catch (Exception e1) {
        return 0.0;
      }
    }
    
    Double DQ = 0.0;

    for (String term : query) {
      Double wij = wij(kvs, 0.4, term, document);
      Double wiq = 1.0;
      try {
        wiq = WIQ.get(term);
      } catch (Exception e) {
      }
      DQ += wij * wiq;
    }
    Double score = DQ / (Math.sqrt(D) * Math.sqrt(Q));
    return score;
  }

  public static Double wiq(KVSClient kvs, Double alpha, String term, ArrayList<String> query) {
    Double tf = normalized_query_TF(alpha, term, query);
    Double idf = idf(kvs, term);
    Double w = tf * idf;
    return w;
  }

  public static double normalized_query_TF(double alpha, String term, ArrayList<String> document) {
    int freq = Collections.frequency(document, term);
    if (freq == 0) {
      return 0.0;
    }
    int maxFreq = 0;
    for (String word : new HashSet<>(document)) {
      int count = Collections.frequency(document, word);
      if (count > maxFreq) {
        maxFreq = count;
      }
    }
    return alpha + (1 - alpha) * freq / (double) maxFreq;
  }

  public static Double wij(KVSClient kvs, Double alpha, String term, String document) {
    try {
      Double wij = wijRDD.get(term).get(document);
      if (wij == null) {
        return  0.0;
      }
      return wij;
    } catch (Exception e) {
      return 0.0;
    }
  }
  // is it really important to have culture difference

  public static Double idf(KVSClient kvs, String term) {
    try {
      Double idf = idfRDD.get(term);
      if (idf == null) {
        return  0.0;
      }      
      return idf;
    } catch (Exception e) {
      return 0.0;
    }
  }


  public static Set<String> getCommonEnglSet() {
    Set<String> wordsSet = new HashSet<>();
    Stemmer s = new Stemmer();
    try {
      // File file = new File("words.txt");
      File file = new File("./src/wordlist.10000");
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

}
