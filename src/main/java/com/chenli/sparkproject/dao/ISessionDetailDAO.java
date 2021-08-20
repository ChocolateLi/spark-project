package com.chenli.sparkproject.dao;

import com.chenli.sparkproject.domain.SessionDetail;

import java.util.List;

/**
 * Session明细DAO接口
 * @author Administrator
 *
 */
public interface ISessionDetailDAO {

    /**
     * 插入一条session明细数据
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);

    /**
     * 插入一批数据
     * @param sessionDetails
     */
    void insertBatch(List<SessionDetail> sessionDetails);

}
