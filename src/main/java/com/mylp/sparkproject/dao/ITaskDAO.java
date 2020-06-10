package com.mylp.sparkproject.dao;

import com.mylp.sparkproject.domain.Task;

public interface ITaskDAO {
	
	/**
	 * 根据主键查询任务
	 * @param taskId
	 * @return
	 */
	Task findById(long taskId);

}
