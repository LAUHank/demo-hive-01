package cn.lhl.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ValueMaskUDF extends UDF{
    public String evaluate(String input,int maxSaveStringLength,String replaceSign) {
          if(input.length()<=maxSaveStringLength){
                 return input;
          }
          return input.substring(0,maxSaveStringLength)+replaceSign;
    }
    public static void main(String[] args) {
          System.out.println(new ValueMaskUDF().evaluate("河北省",2,"..."));
    }
}
