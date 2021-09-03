package com.chenli.sparkproject.spark.session;

import com.chenli.sparkproject.constant.Constants;
import com.chenli.sparkproject.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * session聚合统计Accumulator
 * 自定义累加器
 * Spark Core里面比较高端的技术
 *
 * @author: 小LeetCode~
 **/

/**
 * 假设样本数据集合为simple={1,2,3,4}
 *
 * 执行顺序:
 *
 * 1.调用zero(initialValue)，返回zeroValue
 *
 * 2.调用addAccumulator(zeroValue,1) 返回v1.
 *
 * 调用addAccumulator(v1,2)返回v2.
 *
 * 调用addAccumulator(v2,3)返回v3.
 *
 * 调用addAccumulator(v3,4)返回v4.
 *
 * 3.调用addInPlace(initialValue,v4)
 *
 * 因此最终结果是zeroValue+1+2+3+4+initialValue.
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    private static final long serialVersionUID = 6548616966668061352L;

    /**
     * zero方法，其实主要用于数据的初始化
     * 初始化中，所有范围区间的数量都是0
     * 还是采用key=value|key=value连接串的格式
     * @param v
     * @return
     */
    @Override
    public String zero(String v) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * addInplace和addAcumulator
     * 可以理解为一样的
     *
     * @param r1 我们初始化的那个连接串
     * @param r2 某个session对应的区间
     * @return 在r1中找到r2对应的value，累加1，然后更新回连接串中，并返回
     */
    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1,r2);
    }

    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1,t2);
    }

    /**
     * session统计计算逻辑
     * @param v1 连接串
     * @param v2 范围区间
     * @return 更新以后的连接串
     */
    private String add(String v1, String v2) {
        //校验：v1为空的话，直接返回v2
        if (StringUtils.isEmpty(v1)) {
            return v2;
        }

        //使用StringUtils工具类，从v1中，提取v2对应的值，累加1
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if (oldValue != null) {
            //将范围区间原有的值，累加1
            int newValue = Integer.valueOf(oldValue) + 1;
            // 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }
        return v1;
    }
}
