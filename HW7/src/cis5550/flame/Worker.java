package cis5550.flame;

import java.util.*;
import java.util.function.Function;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {

	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <masterIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, port);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });
    post("/rdd/flatMap", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		Iterable<String> result = lambda.op(r.get("value"));
			if (result != null) {
				for (String item: result) {					
					kvs.put(output_table, String.valueOf(UUID.randomUUID()), "value", item);
				}
	    	}
    	}
    	return "OK";
    });
    post("/pair_rdd/flatMap", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	FlamePairRDD.PairToStringIterable lambda = (FlamePairRDD.PairToStringIterable)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		for (String col: r.columns()) {    			
    			Iterable<String> result = lambda.op(new FlamePair(r.key(), r.get(col)));
    			if (result != null) {
    				for (String item: result) {					
    					kvs.put(output_table, String.valueOf(UUID.randomUUID()), "value", item);
    				}
    			}
    		}		
    	}
    	
    	return "OK";
    });
    post("/rdd/mapToPair", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	FlameRDD.StringToPair lambda = (FlameRDD.StringToPair)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		FlamePair result = lambda.op(r.get("value"));	
			if (result != null) {
//				System.out.println("result: " + result.toString());					
				kvs.put(output_table, result._1(), r.key(), result._2());
	    	}
    	}
    	return "OK";
    });
    post("/pair_rdd/foldByKey", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
//    	System.out.println(kvs.numWorkers());
//    	System.out.println(kvs.count(input_table));
    	FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		String result = request.queryParams("zeroElement");
    		for (String col: r.columns()) {    			
    			result = lambda.op(r.get(col), result);
    		}		
    		kvs.put(output_table, r.key(), "value", result);
    	}
    	return "OK";
    });
    post("/rdd/fold", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	int acc = 0;
    	while (it.hasNext()) {
    		Row r = it.next();
    		String result = request.queryParams("zeroElement");
    		for (String col: r.columns()) {    			
    			result = lambda.op(r.get(col), result);
    		}
    		acc += Integer.parseInt(result);
    	}
    	kvs.put(output_table, "key", "value", String.valueOf(acc));
    	return "OK";
    });
    post("/rdd/groupBy", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	FlameRDD.StringToString lambda = (FlameRDD.StringToString)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	HashMap<String, LinkedList<String>> hashmap = new HashMap<String, LinkedList<String>>();
    	while (it.hasNext()) {
    		Row r = it.next();
    		String result = lambda.op(r.get("value"));
			if (result != null) {
				if (hashmap.containsKey(result)) {					
					hashmap.get(result).add(r.get("value"));
				} else {
					hashmap.put(result, new LinkedList<String>());
					hashmap.get(result).add(r.get("value"));
				}
	    	}
    	}
    	for (String k: hashmap.keySet()) {
    		kvs.put(output_table, k, String.valueOf(UUID.randomUUID()), String.join(",", hashmap.get(k)));
    	}
    	return "OK";
    });
    post("/rdd/sample", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	double f = Double.parseDouble(request.queryParams("zeroElement")); 
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
//    	System.out.println(kvs.count(input_table));
//    	FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		String result = (r.get("value"));
			if (result != null && Math.random() < f) {				
				kvs.put(output_table, r.key(), "value", result);
			}
    	}
    	return "OK";
    });
    post("/rdd/fromTable", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		FlameContext.RowToString lambda = (FlameContext.RowToString)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
			String result = lambda.op(r);
    		kvs.put(output_table, r.key(), "value", result);	
    	}
    	return "OK";
    });
    post("/rdd/flatMapToPair", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	FlameRDD.StringToPairIterable lambda = (FlameRDD.StringToPairIterable)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		Iterable<FlamePair> result = lambda.op(r.get("value"));
			if (result != null) {
				for (FlamePair item: result) {				
					kvs.put(output_table, item._1(), String.valueOf(UUID.randomUUID()), item._2());
				}
	    	}
    	}
    	
    	return "OK";
    });
    post("/pair_rdd/flatMapToPair", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	FlamePairRDD.PairToPairIterable lambda = (FlamePairRDD.PairToPairIterable)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		for (String col: r.columns()) {    			
    			Iterable<FlamePair> result = lambda.op(new FlamePair(r.key(), r.get(col)));
    			if (result != null) {
    				for (FlamePair item: result) {					
    					kvs.put(output_table, item._1(), String.valueOf(UUID.randomUUID()), item._2());
    				}
    			}
    		}
    	}
    	return "OK";
    });
    post("/rdd/distinct", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		kvs.put(output_table, r.get("value"), "value", r.get("value"));
    	}
    	return "OK";
    });
    post("/pair_rdd/join", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	String other = request.queryParams("zeroElement");
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		for (String col: r.columns()) {
    			if (kvs.existsRow(other, r.key())) {
    				for (String col2: kvs.getRow(other, r.key()).columns()) {    					
    					kvs.put(output_table, r.key(), String.valueOf(UUID.randomUUID()), String.join(",", r.get(col), new String(kvs.get(other, r.key(), col2))));
    				}
    			}
    		}
    	}
    	return "OK";
    });
    post("/pair_rdd/cogroup", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	String other = request.queryParams("zeroElement");
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		List<String> X = new LinkedList<String>();
    		for (String col: r.columns()) {
    			X.add(r.get(col));
    		}
    		List<String> Y = new LinkedList<String>();
    		if (kvs.existsRow(other, r.key())) {
    			for (String col2: kvs.getRow(other, r.key()).columns()) {    					
    				Y.add(new String(kvs.get(other, r.key(), col2)));
    			}
    		}
    		kvs.put(output_table, r.key(), "value","[" + String.join(",", X) + "]" + "," + "[" + String.join(",", Y) + "]");
    	}
    	it = kvs.scan(other);
    	while (it.hasNext()) {
    		Row r = it.next();
    		if (kvs.existsRow(output_table, r.key())) {
    			continue;
    		}
    		List<String> Y = new LinkedList<String>();
    		if (kvs.existsRow(other, r.key())) {
    			for (String col2: kvs.getRow(other, r.key()).columns()) {    					
    				Y.add(new String(kvs.get(other, r.key(), col2)));
    			}
    		}
    		kvs.put(output_table, r.key(), "value","[" + "]" + "," + "[" + String.join(",", Y) + "]");
    	}
    	return "OK";
    });
    post("/rdd/filter", (request, response) -> {
    	String input_table = request.queryParams("in");
    	String output_table = request.queryParams("out");
    	String KVSmaster = request.queryParams("KVSmaster");
    	String from = request.queryParams("from");
    	if (from.equals("null")) from = null;
    	String to = request.queryParams("to");
    	if (to.equals("null")) to = null;
    	KVSClient kvs = new KVSClient(KVSmaster);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	FlameRDD.StringToBoolean lambda = (FlameRDD.StringToBoolean)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	while (it.hasNext()) {
    		Row r = it.next();
    		if (lambda.op(r.get("value"))) {
    			kvs.put(output_table, r.key(), "value", r.get("value"));
    		}
    	}
    	return "OK";
    });
	}
}
