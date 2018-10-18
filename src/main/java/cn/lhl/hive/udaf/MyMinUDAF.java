package cn.lhl.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 自定义UDAF - 模拟聚合函数min(int)
 * 
 * @author Administrator
 *
 */
public class MyMinUDAF extends UDAF {
	public static class IntEvaluator implements UDAFEvaluator {

		private Integer result;

		@Override
		public void init() {
			result = null;
		}

		public boolean iterate(Integer val) {
			boolean res = true;
			if (val != null) {
				if (result == null) {
					result = val;
				} else {
					result = Math.min(result, val);
				}
			}
			return res;
		}

		public Integer terminatePartial() {
			return result;
		}

		public boolean merge(Integer val) {
			return iterate(val);
		}

		public Integer terminate() {
			return result;
		}
	}

	public static class DoubleEvaluator implements UDAFEvaluator {

		private Double result;

		@Override
		public void init() {
			result = null;
		}

		public boolean iterate(Double val) {
			boolean res = true;
			if (val != null) {
				if (result == null) {
					result = val;
				} else {
					result = Math.min(result, val);
				}
			}
			return res;
		}

		public Double terminatePartial() {
			return result;
		}

		public boolean merge(Double val) {
			return iterate(val);
		}

		public Double terminate() {
			return result;
		}
	}
}
