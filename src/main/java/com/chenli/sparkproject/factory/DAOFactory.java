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

    /**
     * 获取top10Session的活跃DAO
     * @return
     */
    public static ITop10SessionDAO getTop10SessionDAO(){
        return new Top10SessionDAOImpl();
    }

    /**
     * 获取页面单跳转化率的DAO
     * @return
     */
    public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO(){
        return new PageSplitConvertRateDAOImpl();
    }

    /**
     * 获取各区域top3商品的DAO
     * @return
     */
    public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
        return new AreaTop3ProductDAOImpl();
    }

    /**
     * 获取用户广告点击量的DAO
     * @return
     */
    public static IAdUserClickCountDAO getAdUserClickCountDAO() {
        return new AdUserClickCountDAOImpl();
    }

    /**
     * 获取用户黑名单的DAO
     * @return
     */
    public static IAdBlacklistDAO getAdBlacklistDAO() {
        return new AdBlacklistDAOImpl();
    }

    /**
     * 获取广告实时统计的DAO
     * @return
     */
    public static IAdStatDAO getAdStatDAO() {
        return new AdStatDAOImpl();
    }

    /**
     * 各省top3热门广告
     * @return
     */
    public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
        return new AdProvinceTop3DAOImpl();
    }

    /**
     * 广告点击趋势DAO实现类
     * @return
     */
    public static IAdClickTrendDAO getAdClickTrendDAO() {
        return new AdClickTrendDAOImpl();
    }
}
