package com.mylp.sparkproject.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.mylp.sparkproject.dao.IAdUserClickCountDAO;
import com.mylp.sparkproject.domain.AdUserClickCount;
import com.mylp.sparkproject.jdbc.JDBCHelper;
import com.mylp.sparkproject.jdbc.JDBCHelper.QueryCallback;
import com.mylp.sparkproject.model.QueryResult;

public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {

	@Override
	public void updateBatch(List<AdUserClickCount> adUserClickCounts) {
		List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<AdUserClickCount>();
		List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<AdUserClickCount>();
		
		String selectSql = "select count(*) from ad_user_click_count where date = ? and user_id = ? and ad_id = ?";
		Object[] params = null;
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		for (AdUserClickCount adUserClickCount : adUserClickCounts) {
			final QueryResult queryResult = new QueryResult();
			
			params = new Object[] {
				adUserClickCount.getDate(),
				adUserClickCount.getUserId(),
				adUserClickCount.getAdId()
			};
			
			jdbcHelper.executeQuery(selectSql, params, new QueryCallback() {
				
				@Override
				public void process(ResultSet rs) throws Exception {
					while (rs.next()) {
						int result = rs.getInt(1);
						queryResult.setCount(result);
					}
				}
			});
			
			if (queryResult.getCount() > 0) {
				updateAdUserClickCounts.add(adUserClickCount);
			} else {
				insertAdUserClickCounts.add(adUserClickCount);
			}
		}

		// 执行批量插入
		String insertSql = "insert into ad_user_click_count values(?,?,?,?)";
		List<Object[]> insertParamList = new ArrayList<Object[]>();
		for (AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
			Object[] insertParams = new Object[] {
				adUserClickCount.getDate(),
				adUserClickCount.getUserId(),
				adUserClickCount.getAdId(),
				adUserClickCount.getClickCount()
			};
			insertParamList.add(insertParams);
		}
		jdbcHelper.executeBatch(insertSql, insertParamList);
		
		// 执行批量更新
		String updateSql = "update ad_user_click_count set click_count = ? "
				+ "where date = ? and user_id = ? and ad_id = ?";
		List<Object[]> updateParamList = new ArrayList<Object[]>();
		for (AdUserClickCount adUserClickCount : updateAdUserClickCounts) {
			Object[] updateParams = new Object[] {
				adUserClickCount.getClickCount(),
				adUserClickCount.getDate(),
				adUserClickCount.getUserId(),
				adUserClickCount.getAdId()
			};
			updateParamList.add(updateParams);
		}
		jdbcHelper.executeBatch(updateSql, updateParamList);
	}

	@Override
	public long findClickCountByMultiKey(String date, long userId, long adId) {
		String sql = "select click_count from ad_user_click_count where date = ? and user_id = ? and ad_id = ?";
		Object[] params = new Object[] {
			date, userId, adId
		};
		
		final QueryResult queryResult = new QueryResult();
		JDBCHelper.getInstance().executeQuery(sql, params, new QueryCallback() {
			
			@Override
			public void process(ResultSet rs) throws Exception {
				while (rs.next()) {
					int clickCount = rs.getInt(1);
					queryResult.setCount(clickCount);
				}
				
			}
		});
		
		return (long)queryResult.getCount();
	}

}
