package com.mylp.sparkproject.dao;

import com.mylp.sparkproject.domain.Top10CategorySession;

public interface ITop10CategorySessionDAO {
	
	/**
	 * 插入热门品类top10热门session
	 * @param top10CategorySession
	 */
	void insert(Top10CategorySession top10CategorySession);

}
