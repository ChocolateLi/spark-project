package com.chenli.sparkproject.impl;

import com.chenli.sparkproject.dao.IAreaTop3ProductDAO;
import com.chenli.sparkproject.domain.AreaTop3Product;
import com.chenli.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: 小LeetCode~
 **/
public class AreaTop3ProductDAOImpl implements IAreaTop3ProductDAO {
    @Override
    public void insertBatch(List<AreaTop3Product> areaTopsProducts) {
        String sql = "insert into area_top3 values(?,?,?,?,?,?,?,?)";

        List<Object[]> paramList = new ArrayList<>();

        for (AreaTop3Product areaTop3Product : areaTopsProducts) {
            Object[] params = new Object[8];

            params[0] = areaTop3Product.getTaskid();
            params[1] = areaTop3Product.getArea();
            params[2] = areaTop3Product.getAreaLevel();
            params[3] = areaTop3Product.getProductid();
            params[4] = areaTop3Product.getCityInfos();
            params[5] = areaTop3Product.getClickCount();
            params[6] = areaTop3Product.getProductName();
            params[7] = areaTop3Product.getProductStatus();

            paramList.add(params);

            JDBCHelper jdbcHelper = JDBCHelper.getInstance();
            jdbcHelper.executeBatch(sql, paramList);
        }
    }
}
