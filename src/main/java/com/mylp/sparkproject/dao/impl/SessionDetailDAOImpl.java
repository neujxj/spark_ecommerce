package com.mylp.sparkproject.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.mylp.sparkproject.dao.ISessionDetailDAO;
import com.mylp.sparkproject.domain.SessionDetail;
import com.mylp.sparkproject.jdbc.JDBCHelper;

public class SessionDetailDAOImpl implements ISessionDetailDAO {

	@Override
	public void insert(SessionDetail sessionDetail) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params = new Object[] {sessionDetail.getTaskid(),
				sessionDetail.getUserid(),
				sessionDetail.getSessionid(),
				sessionDetail.getPageid(),
				sessionDetail.getActionTime(),
				sessionDetail.getSearchKeyword(),
				sessionDetail.getClickCategoryId(),
				sessionDetail.getClickProductId(),
				sessionDetail.getOrderCategoryIds(),
				sessionDetail.getOrderProductIds(),
				sessionDetail.getPayCategoryIds(),
				sessionDetail.getPayProductIds()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

	@Override
	public void insertBatch(List<SessionDetail> sessionDetails) {
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
		List<Object[]> paramList = new ArrayList<Object[]>();
		for (SessionDetail sessionDetail : sessionDetails) {
			Object[] params = new Object[] {sessionDetail.getTaskid(),
					sessionDetail.getUserid(),
					sessionDetail.getSessionid(),
					sessionDetail.getPageid(),
					sessionDetail.getActionTime(),
					sessionDetail.getSearchKeyword(),
					sessionDetail.getClickCategoryId(),
					sessionDetail.getClickProductId(),
					sessionDetail.getOrderCategoryIds(),
					sessionDetail.getOrderProductIds(),
					sessionDetail.getPayCategoryIds(),
					sessionDetail.getPayProductIds()};
			paramList.add(params);
		}
			
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramList);
	}

}
