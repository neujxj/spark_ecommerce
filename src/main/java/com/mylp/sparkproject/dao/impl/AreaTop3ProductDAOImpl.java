package com.mylp.sparkproject.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.mylp.sparkproject.dao.IAreaTop3ProductDAO;
import com.mylp.sparkproject.domain.AreaTop3Product;
import com.mylp.sparkproject.jdbc.JDBCHelper;

public class AreaTop3ProductDAOImpl implements IAreaTop3ProductDAO {

	@Override
	public void insert(AreaTop3Product areaTop3Product) {
		String sql = "insert into area_top3_product values(?,?,?,?,?,?,?,?)";
		Object[] params = new Object[] {
				areaTop3Product.getTaskId(),
				areaTop3Product.getArea(),
				areaTop3Product.getAreaLevel(),
				areaTop3Product.getProductId(),
				areaTop3Product.getCityNames(),
				areaTop3Product.getClickCount(),
				areaTop3Product.getProductName(),
				areaTop3Product.getProductStatus()
			};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

	@Override
	public void insertBatch(List<AreaTop3Product> areaTop3Products) {
		String sql = "insert into area_top3_product values(?,?,?,?,?,?,?,?)";
		List<Object[]> paramList = new ArrayList<Object[]>();
		for (AreaTop3Product areaTop3Product : areaTop3Products) {
			Object[] params = new Object[] {
					areaTop3Product.getTaskId(),
					areaTop3Product.getArea(),
					areaTop3Product.getAreaLevel(),
					areaTop3Product.getProductId(),
					areaTop3Product.getCityNames(),
					areaTop3Product.getClickCount(),
					areaTop3Product.getProductName(),
					areaTop3Product.getProductStatus()
				};
			paramList.add(params);
		}
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramList);
	}

}
