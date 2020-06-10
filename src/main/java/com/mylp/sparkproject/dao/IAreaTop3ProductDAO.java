package com.mylp.sparkproject.dao;

import java.util.List;

import com.mylp.sparkproject.domain.AreaTop3Product;

public interface IAreaTop3ProductDAO {
	
	// 插入各区域top3商品数据
	void insert(AreaTop3Product areaTop3Product);

	void insertBatch(List<AreaTop3Product> areaTop3Products);
}
