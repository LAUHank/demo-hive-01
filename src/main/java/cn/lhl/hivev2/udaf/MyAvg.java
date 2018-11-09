package cn.lhl.hivev2.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class MyAvg extends UDAF {
	public static class MyDoubleAvg implements UDAFEvaluator {
		
		public static class MyDoublePR {
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
			pr.sum += val;
			pr.count++;
			return true;
		}
		
		public MyDoublePR terminatePartial() {
			return pr;
		}
		
		public boolean merge(MyDoublePR other) {
			if (other == null) {
				other = new MyDoublePR();
			}
			pr.sum += other.sum;
			pr.count += other.count;
			return true;
		}
		
		public Double terminate() {
			return pr.count==0 ? null : pr.sum / pr.count;
		}
	}
}
