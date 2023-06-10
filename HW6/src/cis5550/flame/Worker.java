package cis5550.flame;

import java.util.*;
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
//    	System.out.println(input_table);
//    	System.out.println(output_table);
//    	System.out.println(KVSmaster);
//    	System.out.println(from);
//    	System.out.println(to);
    	KVSClient kvs = new KVSClient(KVSmaster);
//    	System.out.println(kvs.numWorkers());
//    	System.out.println(kvs.count(input_table));
//    	kvs.put(output_table, "row", "value", "test");
    	FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	while (it.hasNext()) {
    		Row r = it.next();
    		Iterable<String> result = lambda.op(r.get("value"));
			if (result != null) {
				for (String item: result) {
//					System.out.println("result: " + item);					
					kvs.put(output_table, String.valueOf(UUID.randomUUID()), "value", item);
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
//    	System.out.println(kvs.numWorkers());
//    	System.out.println(kvs.count(input_table));
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
    post("/rdd/foldByKey", (request, response) -> {
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
    post("/rdd/groupBy", (request, response) -> {
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
    	FlameRDD.StringToString lambda = (FlameRDD.StringToString)Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
    	Iterator<Row> it = kvs.scan(input_table, from, to);
    	HashMap<String, LinkedList<String>> hashmap = new HashMap<String, LinkedList<String>>();
    	while (it.hasNext()) {
    		Row r = it.next();
    		String result = lambda.op(r.get("value"));
			if (result != null) {
//				System.out.println("result: " + result.toString());					
//				kvs.put(output_table, result, r.key(), r.get("value"));
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
    	System.out.println(f);
    	System.out.println(f*2);
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
	}
}
