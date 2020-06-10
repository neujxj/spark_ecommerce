package com.mylp.sparkproject.dao;

import com.mylp.sparkproject.domain.SessionRandomExtract;

/**
 * session聚合统计模块DAO接口
 * @author Administrator
 *
 */
public interface ISessionRandomExtractDAO {
	
	/**
	 * 插入随机抽取的session
	 * @param sessionRandomExtract
	 */
	void insert(SessionRandomExtract sessionRandomExtract);

}
