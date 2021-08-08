package com.chenli.sparkproject.impl;

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

}
