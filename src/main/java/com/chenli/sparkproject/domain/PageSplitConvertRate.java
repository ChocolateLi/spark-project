package com.chenli.sparkproject.domain;

/**
 * 页面切片转化率实体类
 * @author: 小LeetCode~
 **/
public class PageSplitConvertRate {

    private long taskid;
    private String convertRate;

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public String getConvertRate() {
        return convertRate;
    }

    public void setConvertRate(String convertRate) {
        this.convertRate = convertRate;
    }
}
