package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Base64Tool;
import cis5550.tools.Helper;
import cis5550.tools.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PageRank {

    private static final Logger logger = Logger.getLogger(PageRank.class);

    private static final double DECAY_FACTOR = 0.85;


    public static void run(FlameContext ctx, String[] args) throws IOException {

        KVSClient kvsClient = ctx.getKVS();

        int iteration = 0;
        if (args.length > 0){
            try {
                iteration = Integer.parseInt(args[0]);
            }catch (NumberFormatException e){
                logger.error("Invalid iteration number");
                return;
            }
        }
        //Get master node
        String master = ctx.getKVS().getMaster();

        try {
            FlamePairRDD state = ctx
                    .fromTable("plain_text", row ->{
                        return Helper.normalizeURL(row.get("url")) + ", " + row.get("children_urls");
                    })
                    .mapToPair(str -> {
                        String[] parts = str.split(",");
                        //parts 0 is the url
                        //parts 1 is the children urls
                        String children_urls = parts[1];
                        if (parts.length > 1){
                            String[] result = parts[1].split(" ");
                            result = Arrays.stream(result).map(element -> Base64Tool.encode(element)).toArray(String[]::new);
                            children_urls = String.join(",", result);
                        }
                        return new FlamePair(Base64Tool.encode(parts[0]),children_urls);
                    })
                    .flatMapToPair(pair -> {
                        String url = pair._1();
                        String children_urls = pair._2();
                        return List.of(
                                new FlamePair(url.trim(), "1.0, 1.0" + children_urls)
                        );
                    });

//            Persist the state
//            ctx.getKVS().persist("state");
            kvsClient.persist("state0");
            state.saveAsTable("TempState");
            Iterator<Row> iterator = ctx.getKVS().scan("TempState");
            while (iterator.hasNext()){
                kvsClient.putRow("state0", iterator.next());
            }
            ctx.output("OK");
        } catch (Exception e) {
            ctx.output("FAILED"+e.getMessage());
        }
    }

    public static List<FlamePair> updateState(FlamePair pairs) {
        String url = pairs._1().trim();
        List<FlamePair> result = new ArrayList<>();
        List<String> parts = Arrays.stream(pairs._2().split(", "))
                .map(String::trim)
                .collect(Collectors.toList());
        double rank = Double.parseDouble(parts.get(0));

        boolean foundSelf = false;
        for (int i = 2; i < parts.size(); i++) {
            if (parts.get(i).equals(url)) {
                foundSelf = true;
            }
            double updatedRank = DECAY_FACTOR * rank / (parts.size() - 2);
            result.add(new FlamePair(parts.get(i), String.valueOf(updatedRank)));
        }

        // If the page has no outgoing links, add itself to the list
        if (!foundSelf) {
            result.add(new FlamePair(url, "0.0"));
        }
        return result;
    }

    public static List<FlamePair> aggregateState(FlamePair pair){
        String[] parts = pair._2().split(",");
        double oldRank = Double.parseDouble(parts[0]);
        double newRank = Double.parseDouble(parts[parts.length - 1]) + (1 - DECAY_FACTOR);
        logger.debug("Rank change: " + oldRank + " -> " + newRank);
        String urls = String.join(", ", Arrays.copyOfRange(parts, 2, parts.length - 1));

        String resultValue = newRank + ", " + oldRank + (urls.isEmpty() ? "" : ", " + urls);
        return Collections.singletonList(new FlamePair(pair._1(), resultValue));
    }

}

