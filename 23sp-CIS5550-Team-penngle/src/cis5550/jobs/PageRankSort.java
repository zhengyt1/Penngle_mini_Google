package cis5550.jobs;

import java.io.FileWriter;
import java.io.IOException;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Base64Tool;

import java.io.IOException;
import java.util.*;

public class PageRankSort {

    public static void run(FlameContext ctx, String[] args) throws IOException {

        //Set iteration times;
        //In this iteration, we will get the state from the previous iteration
        //and calculate the new state
//        if (args.length < 1){
//            ctx.output("ERROR");
//            return;
//        }
        try {
            KVSClient kvs = ctx.getKVS();
            ArrayList<RankPair> readList = new ArrayList<>();
            Iterator<Row> kvsIterator = kvs.scan("pagerank3");
            while (kvsIterator.hasNext()){
                Row row = kvsIterator.next();
                RankPair pair = new RankPair();
                pair.rank = Double.parseDouble(row.get("rank"));
                pair.url = row.key();
                readList.add(pair);
            }
            //read pagerank table
//            FlameRDD rdd = ctx.fromTable("pagerank3", row -> {
//                RankPair pair = new RankPair();
//                pair.rank = Double.parseDouble(row.get("rank"));
//                pair.url = row.key();
//                readList.add(pair);
//                // Don't need the value
//                return pair.url;
//            });
            ctx.output(String.valueOf(readList.size()));
            List<RankPair> list = readList.stream().
                    sorted(
                            (o1, o2) -> Double.compare(o2.rank, o1.rank)
                    ).toList();
            FileWriter writer = new FileWriter("output.txt");
            for (RankPair item : list) {
                writer.write(item + "\n");
            }
            writer.close();
            ctx.output("OK");
        }catch (Exception e){
            ctx.output("ERROR: "+e.getMessage());
        }
    }
}
