package cn.lhl.hivev2.udaf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class MyAvg extends UDAF {
	
	//MyDoubleAvg必须是一个public static的类
	public static class MyDoubleAvg implements UDAFEvaluator {
		
		//MyDoublePR Hadoop会自动进行序列化
		private static class MyDoublePR {
			public Double sum = 0.0;
			public Integer count = 0;
		}

		private MyDoublePR pr;
		
		public MyDoubleAvg() {
			init();
		}
		
		@Override
		public void init() {
			pr = new MyDoublePR();
		}
		
		public boolean iterate(Double val) {
			if (val == null) {
				return true;
			}
			pr.sum += val;
			pr.count++;
			return true;
		}
		
		public MyDoublePR terminatePartial() {
			return pr;
		}
		
		public boolean merge(MyDoublePR other) {
			if (other == null) {
				return true;
			}
			pr.sum += other.sum;
			pr.count += other.count;
			return true;
		}
		
		public Double terminate() {
			return pr.count==0 ? null : pr.sum / pr.count;
		}
	}
	
	//Integer重载, 使用HashMap实现
	public static class MyIntAvg implements UDAFEvaluator {
		
		public MyIntAvg() {
			init();
		}
		
		private Map<String, Integer> map;
		
		@Override
		public void init() {
			map = new HashMap<String, Integer>();
			map.put("sum", 0);
			map.put("count", 0);
		}
		
		public boolean iterate(Integer val) {
			if (val == null) {
				return true;
			}
			map.put("sum", map.get("sum") + val);
			map.put("count", map.get("count") + 1);
			return true;
		}
		
		public Map<String, Integer> terminatePartial() {
			return map;
		}
		
		public boolean merge(Map<String, Integer> other) {
			if (other == null) {
				return true;
			}
			map.put("sum", map.get("sum") + other.get("sum"));
			map.put("count", map.get("count") + other.get("count"));
			return true;
		}
		
		public Integer terminate() {
			return map.get("count")==0 ? null : map.get("sum") / map.get("count");
		}
	}
}
