package cis5550.jobs;

import java.io.IOException;
import java.util.*;
import cis5550.flame.*;
import cis5550.kvs.KVS;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class D {

  public static void run(FlameContext ctx, String[] args) throws Exception {
    KVSClient kvs = ctx.getKVS();
    kvs.persist("D");

    Iterator<Row> iter = kvs.scan("wij");
    while (iter.hasNext()) {
      Row row = iter.next();
      String url = row.key();
      Double D = 0.0;
      Row resultRow = new Row(url);
      for (String col : row.columns()) {
        try {
          Double wij = Double.parseDouble(row.get(col));
          if (wij > 0.0)
            D += wij * wij;
        } catch (Exception e) {
        }
      }
      resultRow.put("acc", ""+D);
      kvs.putRow("D", resultRow);
    }
  }


}