package com.chenli.sparkproject.dao;

import com.chenli.sparkproject.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 */
public interface ISessionRandomExtractDAO {

    /**
     * 插入session随机抽取
     * @param sessionRandomExtract
     */
    void insert(SessionRandomExtract sessionRandomExtract);
}
