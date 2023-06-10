package cis5550.jobs;

import java.util.*;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;

public class Indexer {
	public static void run(FlameContext ctx, String[] arr) {
		ctx.output("in Indexer run");
		try {
			FlamePairRDD flame = ctx.fromTable("crawl", r -> r.get("url") +","+r.get("page"))
					.mapToPair(s -> {
						int i = s.indexOf(",");
						return new FlamePair(s.substring(0,i), s.substring(i+1));
					});
			FlamePairRDD word2url = flame.flatMapToPair(p -> {
				String url = p._1();
				String page = p._2();
				page = page.replaceAll("<.*?>", "").replaceAll("[.,:;!?’\"()-]", "").toLowerCase();
				HashSet<FlamePair> ret = new HashSet<FlamePair>();
				for (String w: page.split("\\s+")) {
					ret.add(new FlamePair(w.replaceAll(" ", ""), url));
//					System.out.println(w.replaceAll(" ", "")+" "+ url);
				}
				return ret;
			});
//			System.out.println(word2url.collect());
//			word2url.saveAsTable("w2u");
			word2url.foldByKey("", (s1,s2) -> s1 + (s1.equals("")?"":",") + s2).saveAsTable("index");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
