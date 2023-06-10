package cis5550.flame;

import java.util.*;

import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {
	public String table_name = "";
	FlamePairRDDImpl(String tn) {
		this.table_name = tn;
	}
	@Override
	public List<FlamePair> collect() throws Exception {
		// TODO Auto-generated method stub
		List<FlamePair> ret = new LinkedList<FlamePair>();
		Iterator<Row> it = Master.kvs.scan(table_name);
		while (it.hasNext()) {
			Row r = it.next();
			for (String col: r.columns()) {				
				ret.add(new FlamePair(r.key(), r.get(col)));
			}
		}
		return ret;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		// TODO Auto-generated method stub
		String new_table_name = FlameContextImpl.invokeOperation("/rdd/foldByKey", Serializer.objectToByteArray(lambda), table_name, zeroElement);
		
		return new FlamePairRDDImpl(new_table_name);
	}

}
