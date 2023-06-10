package cis5550.flame;

import java.util.*;

import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Partitioner;
import cis5550.tools.Partitioner.Partition;
import cis5550.tools.Serializer;

public class FlameContextImpl implements FlameContext {
	public String out = "";
	public static String jarName = "";
	public static String path = "";
	public static int keyRangesPerWorker = 1;
	public static int seq_number = 1;
	public static Vector<String> workers;
	public static KVSClient kvs;
	
	FlameContextImpl(String jarName, KVSClient kvs, String path, Vector<String> workers) {
		FlameContextImpl.jarName = jarName;
		FlameContextImpl.kvs = kvs;
		FlameContextImpl.path = path;
		FlameContextImpl.workers = workers;
	}
	
	public KVSClient getKVS() {
		return kvs;
	}


	// When a job invokes output(), your solution should store the provided string
	// and return it in the body of the /submit response, if and when the job 
	// terminates normally. If a job invokes output() more than once, the strings
	// should be concatenated. If a job never invokes output(), the body of the
	// /submit response should contain a message saying that there was no output.

	public void output(String s) {
//		System.out.println("output: "+s);
		out = out.concat(s);
	}

	// This function should return a FlameRDD that contains the strings in the provided
	// List. It is okay for this method to run directly on the master; it does not
	// need to be parallelized.
	public FlameRDD parallelize(List<String> list) throws Exception {
		String table_name = String.valueOf(System.currentTimeMillis()) + "_" + String.valueOf(seq_number++);
		int i = 0;
		for (String item: list) {			
			kvs.put(table_name, String.valueOf(i++), "value", item);
		}
		FlameRDDImpl flameRDD = new FlameRDDImpl(table_name);
		return flameRDD;
	}
	public static String invokeOperation(String op_name, byte[] lambda, String old_table, String zeroElement) {
		String new_table_name = String.valueOf(System.currentTimeMillis()) + "_" + String.valueOf(seq_number++);
		try {
			Partitioner p = new Partitioner();
			p.setKeyRangesPerWorker(keyRangesPerWorker);
			int num = kvs.numWorkers();
			System.out.println("num of workers "+num);
			if (num == 1) {
//				System.out.println("num "+num);
				p.addKVSWorker(kvs.getWorkerAddress(0), null, null);
			} else {				
				for (int i=0; i<num-1; i++) {
					if (i == 0) p.addKVSWorker(kvs.getWorkerAddress(i), null, kvs.getWorkerID(i+1));
					else p.addKVSWorker(kvs.getWorkerAddress(i), kvs.getWorkerID(i), kvs.getWorkerID(i+1));
				}
				p.addKVSWorker(kvs.getWorkerAddress(num-1), kvs.getWorkerID(num-1), null);
			}
			
			for (String worker: workers) {
				p.addFlameWorker(worker);
			}
			Vector<Partition> result = p.assignPartitions();
//			for (Partition x : result)
//				System.out.println("x: "+x);
			Thread threads[] = new Thread[workers.size()];
			String results[] = new String[workers.size()];
			for (int i=0; i<workers.size(); i++) {
		        final String url = "http://"+workers.elementAt(i)+op_name+"?in="+old_table
		        		+"&out="+new_table_name+"&KVSmaster="+kvs.getMaster()+"&zeroElement="+zeroElement
		        		+"&from="+result.elementAt(i).fromKey+"&to="+result.elementAt(i).toKeyExclusive;
		        final int j = i;
		        threads[i] = new Thread(op_name+(i+1)) {
		          public void run() {
		            try {
		              results[j] = new String(HTTP.doRequest("POST", url, lambda).body());
		            } catch (Exception e) {
		              results[j] = "Exception: "+e;
		              e.printStackTrace();
		            }
		          }
		        };
		        threads[i].start();
		    }

		    // Wait for all the uploads to finish

		    for (int i=0; i<threads.length; i++) {
		        try {
		            threads[i].join();
		        } catch (InterruptedException ie) {
		        }
		    }
		} catch(Exception e) {
			
		}
		return new_table_name;
	}

	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		// TODO Auto-generated method stub
		String table = invokeOperation("/rdd/fromTable", Serializer.objectToByteArray(lambda), tableName, null);
		return new FlameRDDImpl(table);
	}

	@Override
	public void setConcurrencyLevel(int keyRangesPerWorker) {
		// TODO Auto-generated method stub
		FlameContextImpl.keyRangesPerWorker = keyRangesPerWorker;
	}
}
