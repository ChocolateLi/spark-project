package com.chenli.sparkproject.dao;

import com.chenli.sparkproject.domain.SessionAggrStat;

/**
 * session聚合统计模块DAO接口
 */
public interface ISessionAggrStatDAO {

    /**
     * 插入session聚合统计结果
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);
}
