package com.chenli.sparkproject.impl;

import com.chenli.sparkproject.dao.ISessionAggrStatDAO;
import com.chenli.sparkproject.dao.ITaskDAO;

/**
 *  DAO工厂类
 * @author: 小LeetCode~
 **/
public class DAOFactory {

    /**
     * 获取任务管理DAO
     * @return
     */
    public static ITaskDAO getTaskDAO(){
        return new TaskDAOImpl();
    }

    /**
     * 获取session聚合统计DAO
     * @return
     */
    public static ISessionAggrStatDAO getSessionAggrStatDAO(){
        return new SessionAggrStatDAOImpl();
    }

}
