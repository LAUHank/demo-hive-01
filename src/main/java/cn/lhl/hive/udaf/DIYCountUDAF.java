package cn.lhl.hive.udaf;


import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.log4j.Logger;
/**
* 自行实现sql的count操作
*/
//主类继承UDAF
public class DIYCountUDAF extends UDAF {  
    //日志对象初始化,使访类有输出日志的能力
    public static Logger logger=Logger.getLogger(DIYCountUDAF.class);
    
    //静态类实现UDAFEvaluator
    public static class Evaluator implements UDAFEvaluator {  
        //设置成员变量，存储每个统计范围内的总记录数
        private int totalRecords;  
        //初始化函数,map和reduce均会执行该函数,起到初始化所需要的变量的作用
        public Evaluator() {  
            init();  
        }  
        //初始化，初始值为0,并日志记录下相应输出
        public void init() {  
            totalRecords = 0;  
            logger.info("init totalRecords="+totalRecords);
        }  
        //map阶段，返回值为boolean类型，当为true则程序继续执行，当为false则程序退出  
        public boolean iterate(String input) {
            //当input输入不为空的时候，即为有值存在,即为存在1行，故做+1操作
            if (input != null) {  
                totalRecords += 1;  
            }  
            //输出当前组处理到第多少条数据了
            logger.info("iterate totalRecords="+totalRecords);
            return true;  
        }  
        /**
         * 类似于combiner,在map范围内做部分聚合，将结果传给merge函数中的形参mapOutput  
         * 如果需要聚合，则对iterator返回的结果处理，否则直接返回iterator的结果即可
         */
        public int terminatePartial() {  
            logger.info("terminatePartial totalRecords="+totalRecords);
            return totalRecords;  
        }
        
        // reduce 阶段，用于逐个迭代处理map当中每个不同key对应的 terminatePartial的结果
        public boolean merge(int mapOutput) {  
            totalRecords +=mapOutput;  
            logger.info("merge totalRecords="+totalRecords);
            return true;  
        }  
        //处理merge计算完成后的结果，此时的count在merge完成时候，结果已经得出，无需再进一次对整体结果做处理，故直接返回即可
        public int terminate() {  
            logger.info("terminate totalRecords="+totalRecords);
            return totalRecords;  
        }  
    }  
}
