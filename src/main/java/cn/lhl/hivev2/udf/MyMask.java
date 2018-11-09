package cn.lhl.hivev2.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MyMask extends UDF {
	
	private Text result = new Text();
	
	public Text evaluate(String val, Integer len, String maskStr) {
		if (val == null) {
			return null;
		}
		if (val.length() <= len) {
			result.set(val);
		} else {
			String string = val.substring(0, len) + maskStr;
			result.set(string);
		}
		return result;
	}
}
