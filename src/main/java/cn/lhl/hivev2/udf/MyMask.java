package cn.lhl.hivev2.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MyMask extends UDF {
	
	public String evaluate(String val, Integer len, String maskStr) {
		String result = null;
		if (val == null) {
			return null;
		}
		if (val.length() <= len) {
			result = val;
		} else {
			result = val.substring(0, len) + maskStr;
		}
		return result;
	}
}
