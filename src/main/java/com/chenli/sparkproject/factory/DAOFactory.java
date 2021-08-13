package com.chenli.sparkproject.factory;

import com.chenli.sparkproject.dao.*;
import com.chenli.sparkproject.impl.*;

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

    /**
     * 获取随机抽取session的DAO
     * @return
     */
    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }

    /**
     * 获取session明细的DAO
     * @return
     */
    public static ISessionDetailDAO getSessionDetailDAO(){
        return new SessionDetailDAOImpl();
    }

    /**
     * 获取top10Catogory的DAO
     * @return
     */
    public static ITop10CategoryDAO getTop10CategoryDAO(){
        return new Top10CategoryDAOImpl();
    }

}
