package cis5550.flame;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import cis5550.flame.*;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;

public class FlameRDDImpl implements FlameRDD {
	public String table_name = "";

	FlameRDDImpl(String tn) {
		this.table_name = tn;
	}
	@Override
	public List<String> collect() throws Exception {
		// TODO Auto-generated method stub
		List<String> ret = new LinkedList<String>();
		Iterator<Row> it = Master.kvs.scan(table_name);
		while (it.hasNext()) {
			ret.add(it.next().get("value"));
		}
		return ret;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		// TODO Auto-generated method stub
		String new_table_name = FlameContextImpl.invokeOperation("/rdd/flatMap", Serializer.objectToByteArray(lambda), table_name, null);
		return new FlameRDDImpl(new_table_name);
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		// TODO Auto-generated method stub
		String new_table_name = FlameContextImpl.invokeOperation("/rdd/mapToPair", Serializer.objectToByteArray(lambda), table_name, null);
		return new FlamePairRDDImpl(new_table_name);
	}

	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FlameRDD sample(double f) throws Exception {
		// TODO Auto-generated method stub
		String new_table_name = FlameContextImpl.invokeOperation("/rdd/sample", null, table_name, String.valueOf(f));
		return new FlameRDDImpl(new_table_name);
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		// TODO Auto-generated method stub
		String new_table_name = FlameContextImpl.invokeOperation("/rdd/groupBy", Serializer.objectToByteArray(lambda), table_name, null);
		return new FlamePairRDDImpl(new_table_name);
	}
	@Override
	public int count() throws Exception {
		// TODO Auto-generated method stub
		return Master.kvs.count(table_name);
	}
	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		// TODO Auto-generated method stub
		Master.kvs.rename(table_name, tableNameArg);
		this.table_name = tableNameArg;
	}
	@Override
	public FlameRDD distinct() throws Exception {
		// TODO Auto-generated method stub
		String new_table_name = FlameContextImpl.invokeOperation("/rdd/distinct", null, table_name, null);
		return new FlameRDDImpl(new_table_name);
	}
	@Override
	public Vector<String> take(int num) throws Exception {
		// TODO Auto-generated method stub
		Vector<String> ret = new Vector<String>();
		int counter = 0;
		Iterator<Row> it = Master.kvs.scan(table_name);
		while (it.hasNext() && counter < num) {
			counter++;
			ret.add(it.next().get("value"));
			
		}
		return ret;
	}
	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		// TODO Auto-generated method stub
		String new_table_name = FlameContextImpl.invokeOperation("/rdd/fold", Serializer.objectToByteArray(lambda), table_name, zeroElement);
		return new String(Master.kvs.get(new_table_name, "key", "value"));
	}
	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		// TODO Auto-generated method stub
		String new_table_name = FlameContextImpl.invokeOperation("/rdd/flatMapToPair", Serializer.objectToByteArray(lambda), table_name, null);
		return new FlamePairRDDImpl(new_table_name);
	}
	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		// TODO Auto-generated method stub
		String new_table_name = FlameContextImpl.invokeOperation("/rdd/filter", Serializer.objectToByteArray(lambda), table_name, null);
		return new FlameRDDImpl(new_table_name);
	}
	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		// TODO Auto-generated method stub
//		String new_table_name = FlameContextImpl.invokeOperation("/rdd/mapPartitions", Serializer.objectToByteArray(lambda), table_name, null);
//		return new FlameRDDImpl(new_table_name);
		return null;
	}

}
