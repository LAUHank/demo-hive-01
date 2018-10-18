package cn.lhl.hive.udaf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.log4j.Logger;
/**
* 实现多条数据合并成一条数据
*/
// 主类继承UDAF
public class StudentScoreAggUDAF extends UDAF {
    // 日志对象初始化
    public static Logger logger = Logger.getLogger(StudentScoreAggUDAF.class);
    // 静态类实现UDAFEvaluator
    public static class Evaluator implements UDAFEvaluator {
        // 设置成员变量，存储每个统计范围内的总记录数
        private Map<String, String> courseScoreMap;
        
        //初始化函数,map和reduce均会执行该函数,起到初始化所需要的变量的作用
        public Evaluator() {
            init();
        }
        // 初始化函数间传递的中间变量
        public void init() {
            courseScoreMap = new HashMap<String, String>();
        }
        
         //map阶段，返回值为boolean类型，当为true则程序继续执行，当为false则程序退出  
        public boolean iterate(String course, String score) {
            if (course == null || score == null) {
                return true;
            }
            courseScoreMap.put(course, score);
            return true;
        }
         /**
         * 类似于combiner,在map范围内做部分聚合，将结果传给merge函数中的形参mapOutput  
         * 如果需要聚合，则对iterator返回的结果处理，否则直接返回iterator的结果即可
         */
        public Map<String, String> terminatePartial() {
            return courseScoreMap;
        }
         // reduce 阶段，用于逐个迭代处理map当中每个不同key对应的 terminatePartial的结果
        public boolean merge(Map<String, String> mapOutput) {
            this.courseScoreMap.putAll(mapOutput);
            return true;
        }
        // 处理merge计算完成后的结果，即对merge完成后的结果做最后的业务处理
        public String terminate() {
            return courseScoreMap.toString();
        }
    }
}
