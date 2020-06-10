package com.mylp.sparkproject.dao;

import java.util.List;

import com.mylp.sparkproject.domain.SessionDetail;

/**
 * session聚合统计模块DAO接口
 * @author Administrator
 *
 */
public interface ISessionDetailDAO {
	
	/**
	 * 插入session明细数据
	 * @param sessionAggrStat
	 */
	void insert(SessionDetail sessionDetail);
	
	void insertBatch(List<SessionDetail> sessionDetails);

}
