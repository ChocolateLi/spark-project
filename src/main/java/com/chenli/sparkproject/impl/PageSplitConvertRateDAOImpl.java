package com.chenli.sparkproject.impl;

import com.chenli.sparkproject.dao.IPageSplitConvertRateDAO;
import com.chenli.sparkproject.domain.PageSplitConvertRate;
import com.chenli.sparkproject.jdbc.JDBCHelper;

/**
 * 页面切片转化率DAO实现类
 * @author: 小LeetCode~
 **/
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {
    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "insert into page_split_convert_rate values(?,?)";
        Object[] params = new Object[]{pageSplitConvertRate.getTaskid(),
                pageSplitConvertRate.getConvertRate()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
