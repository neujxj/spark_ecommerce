package com.mylp.sparkproject.dao.impl;

import com.mylp.sparkproject.dao.ITop10CategorySessionDAO;
import com.mylp.sparkproject.domain.Top10CategorySession;
import com.mylp.sparkproject.jdbc.JDBCHelper;

public class Top10CategorySessionDAOImpl implements ITop10CategorySessionDAO {

	@Override
	public void insert(Top10CategorySession top10CategorySession) {
		
		String sql = "insert into top10_category_session values(?,?,?,?)";
		Object[] params = {
				top10CategorySession.getTaskId(),
				top10CategorySession.getCategoryId(),
				top10CategorySession.getSessionId(),
				top10CategorySession.getClickCount()
		};

		JDBCHelper.getInstance().executeUpdate(sql, params);
	}

}
