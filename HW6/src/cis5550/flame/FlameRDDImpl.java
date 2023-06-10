package cis5550.flame;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import cis5550.flame.*;

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

}
