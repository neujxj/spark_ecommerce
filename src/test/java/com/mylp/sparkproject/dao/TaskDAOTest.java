package com.mylp.sparkproject.dao;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.mylp.sparkproject.dao.factory.DAOFactory;
import com.mylp.sparkproject.domain.Task;

public class TaskDAOTest {
	
	@Test
	public void findById() {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(1);
		assertEquals(task.getTaskId(), 1);
	}

}
