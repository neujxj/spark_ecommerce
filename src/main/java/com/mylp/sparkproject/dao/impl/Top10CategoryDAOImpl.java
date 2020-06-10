package com.mylp.sparkproject.dao.impl;

import com.mylp.sparkproject.dao.ITop10CategoryDAO;
import com.mylp.sparkproject.domain.Top10Category;
import com.mylp.sparkproject.jdbc.JDBCHelper;

public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

	@Override
	public void insert(Top10Category top10Category) {
		String sql = "insert into top10_category values(?,?,?,?,?)";
		Object[] params = new Object[] {
			top10Category.getTaskId(),
			top10Category.getCategoryId(),
			top10Category.getClickCount(),
			top10Category.getOrderCount(),
			top10Category.getPayCount()
		};

		JDBCHelper.getInstance().executeUpdate(sql, params);
	}

}
