package com.mylp.sparkproject.dao;

import java.util.List;

import com.mylp.sparkproject.domain.AdUserClickCount;

public interface IAdUserClickCountDAO {

	// 更新用户广告点击次数
	void updateBatch(List<AdUserClickCount> adUserClickCounts);
}
