package com.mylp.sparkproject.dao.impl;

import java.sql.ResultSet;

import com.mylp.sparkproject.dao.ITaskDAO;
import com.mylp.sparkproject.domain.Task;
import com.mylp.sparkproject.jdbc.JDBCHelper;

public class TaskDAOImpl implements ITaskDAO {

	@Override
	public Task findById(long taskId) {
		final Task task = new Task();
		
		String sql = "select * from task where task_id=?";
		Object[] params = new Object[] { taskId };
		
		JDBCHelper.getInstance().executeQuery(sql, params, new JDBCHelper.QueryCallback() {
			
			@Override
			public void process(ResultSet rs) throws Exception {
				if (rs.next()) {
					task.setTaskId(rs.getLong(1));
					task.setTaskName(rs.getString(2));
					task.setCreateTime(rs.getString(3));
					task.setStartTime(rs.getString(4));
					task.setFinishTime(rs.getString(5));
					task.setTaskType(rs.getString(6));
					task.setTaskStatus(rs.getString(7));
					task.setTaskParam(rs.getString(8));
				}	
			}
		});
		
		if (task.getTaskId() == taskId)
			return task;
		else
			return null;
	}

}
