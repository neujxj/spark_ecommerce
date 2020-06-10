package com.mylp.sparkproject.dao;

import java.util.List;

import com.mylp.sparkproject.domain.AdUserClickCount;

public interface IAdUserClickCountDAO {
	
	// 根据date，userId，adId查找clickCount
	long findClickCountByMultiKey(String date, long userId, long adId);

	// 更新用户广告点击次数
	void updateBatch(List<AdUserClickCount> adUserClickCounts);
}
