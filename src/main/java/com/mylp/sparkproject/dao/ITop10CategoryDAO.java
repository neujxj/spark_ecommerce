package com.mylp.sparkproject.dao;

import com.mylp.sparkproject.domain.Top10Category;

public interface ITop10CategoryDAO {
	
	/**
	 * 插入top10品类点击信息
	 * @param top10Category
	 */
	void insert(Top10Category top10Category);

}
