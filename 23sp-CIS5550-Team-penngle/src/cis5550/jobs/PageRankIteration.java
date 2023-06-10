package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.kvs.KVS;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Base64Tool;
import cis5550.tools.Hasher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PageRankIteration {
    public static void run(FlameContext ctx, String[] args) throws IOException {

        //Set iteration times;
        //In this iteration, we will get the state from the previous iteration
        //and calculate the new state
        try {
            int time = 0;
            String table_name = "state";
            try{
                time = Integer.parseInt(args[0]);
            }catch (NumberFormatException e){
                ctx.output("ERROR");
                return;
            }

            FlamePairRDD state = ctx.fromTable(table_name+time, row -> {
                return row.key()+","+row.get(row.key());
            }).mapToPair(str -> {
                String[] parts = str.split(",");
                String value = String.join(",", Arrays.copyOfRange(parts,1,parts.length));
                return new FlamePair(parts[0],value);
            });
            int iteration = 0;
            while (iteration < 2) {
                FlamePairRDD transferState = state.flatMapToPair(PageRank::updateState);

                FlamePairRDD aggregateState = transferState.foldByKey("0.0", (a, b) -> String.valueOf(Double.parseDouble(a) + Double.parseDouble(b)));

                state = state.join(aggregateState)
                        .flatMapToPair(PageRank::aggregateState);

                iteration++;
            }
            //save state result to kvs
            String new_table_name = table_name+(time+1);
            ctx.getKVS().persist(new_table_name);
            state.saveAsTable("TempState"+(time));
            Iterator<Row> iterator = ctx.getKVS().scan("TempState"+(time));
            while (iterator.hasNext()){
                ctx.getKVS().putRow(new_table_name, iterator.next());
            }

            //output pagerank result to kvs
            String pagerank_table_name = "pagerank"+(time);
            ctx.getKVS().persist(pagerank_table_name);
            state.flatMapToPair(pair -> {
                KVSClient kvs = ctx.getKVS();
                String[] parts = pair._2().split(", ");
                String decodeUrl = Base64Tool.decode(pair._1());
                Row row = new Row(Hasher.hash(decodeUrl));
                row.put("rank", parts[0]);
                kvs.putRow(pagerank_table_name, row);
                return List.of(new FlamePair(pair._1(), parts[0]));
            });

            ctx.output("OK");
        }catch (Exception e){
            ctx.output("ERROR");
        }
    }

}
