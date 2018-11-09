package cn.lhl.hivev2.udaf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class MyGroupConcat extends UDAF {
	public static class MyGC implements UDAFEvaluator {
		
		public MyGC() {
			init();
		}
		
		private Map<String, Double> map = new TreeMap<String, Double>();
		
		@Override
		public void init() {
			
		}
		
		public boolean iterate(String k, Double v) {
			map.put(k, v);
			return true;
		}
		
		public Map<String, Double> terminatePartial() {
			return map;
		}
		
		public boolean merge(Map<String, Double> other) {
			map.putAll(other);
			return true;
		}
		
		public List<String> terminate() {
			List<String> list = new ArrayList<String>();
			for (Map.Entry<String, Double> me : map.entrySet()) {
				list.add(me.getKey() + "=" + me.getValue());
			}
			return list;
		}
	}
}
