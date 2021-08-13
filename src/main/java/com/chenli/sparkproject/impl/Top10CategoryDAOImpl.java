package com.chenli.sparkproject.impl;

import com.chenli.sparkproject.dao.ITop10CategoryDAO;
import com.chenli.sparkproject.domain.Top10Category;
import com.chenli.sparkproject.jdbc.JDBCHelper;

/**
 * top10品类DAO实现
 * @author: 小LeetCode~
 **/
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {
    @Override
    public void insert(Top10Category category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{
          category.getTaskid(),
          category.getCategoryid(),
          category.getClickCount(),
          category.getOrderCount(),
          category.getPayCount()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
