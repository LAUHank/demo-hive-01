package cn.lhl.hivev2.udaf;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class MyGroupConcatV1 extends UDAF {
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
		
		public Map<String, Double> terminate() {
			return map;
		}
	}
}
