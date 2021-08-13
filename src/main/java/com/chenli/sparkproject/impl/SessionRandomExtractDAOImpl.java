package com.chenli.sparkproject.impl;

import com.chenli.sparkproject.dao.ISessionRandomExtractDAO;
import com.chenli.sparkproject.domain.SessionRandomExtract;
import com.chenli.sparkproject.jdbc.JDBCHelper;

/**
 * 随机抽取session的DAO实现
 * @author: 小LeetCode~
 **/
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";

        Object[] params = new Object[]{sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
