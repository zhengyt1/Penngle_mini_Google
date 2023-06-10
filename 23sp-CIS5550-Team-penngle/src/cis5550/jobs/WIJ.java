package cis5550.jobs;

import java.io.IOException;
import java.util.*;
import cis5550.flame.*;
import cis5550.kvs.KVS;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class WIJ {

  public static void run(FlameContext ctx, String[] args) throws Exception {
    KVSClient kvs = ctx.getKVS();
    kvs.persist("wij");

    Iterator<Row> iter = kvs.scan("maxFreqRDD");
    while (iter.hasNext()) {
      Row row = iter.next();
      String url = row.key();
      Double D = 0.0;
      Row resultRow = new Row(url);
      for (String word : getDocument(kvs, Hasher.hash(url), "page").split("\\s+")) {
        try {
          Double wij = wij(kvs, 0.3, word, url);
          resultRow.put(word, ""+wij);     
        } catch (Exception e) {
        }
      }
      kvs.putRow("wij", resultRow);
    }
  }

  public static String getDocument(KVSClient kvs, String document, String term) {
    byte[] doc_b;
    try {
      doc_b = kvs.get("crawl", document, term);
    } catch (IOException e) {
      return "";
    }
    if (doc_b == null) return "";
    return new String(doc_b);
  }

  public static Double wij(KVSClient kvs, Double alpha, String term, String document) {
    Double tf = normalized_doc_tf(kvs, alpha, term, document);
    Double idf = idf(kvs, term);
    Double w = tf * idf;
    return w;
  }

  public static Double idf(KVSClient kvs, String term) {
    Double idf = getValue(kvs, "IDFRDD", term, "acc");
    return idf;
  }
  
  public static Double normalized_doc_tf(KVSClient kvs, Double alpha, String term, String document) {
    Double freq = getValue(kvs, "freqRDD", document, term);
    Double max_freq = getValue(kvs, "maxFreqRDD", document, "acc");
    if (max_freq == 0.0) return 0.0;
    Double tf = alpha + (1 - alpha) * freq / max_freq;
    return tf;
  }

  public static Double getValue(KVSClient kvs, String table, String document, String term) {
    byte[] freq_b;
    try {
      freq_b = kvs.get(table, document, term);
    } catch (IOException e) {
      return 0.0;
    }
    if (freq_b == null) return 0.0;
    return Double.parseDouble(new String(freq_b));
  }

}