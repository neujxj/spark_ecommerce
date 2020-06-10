package com.mylp.sparkproject.dao.factory;

import com.mylp.sparkproject.dao.IAdBlacklistDAO;
import com.mylp.sparkproject.dao.IAdUserClickCountDAO;
import com.mylp.sparkproject.dao.IAreaTop3ProductDAO;
import com.mylp.sparkproject.dao.IPageSplitConvertRateDAO;
import com.mylp.sparkproject.dao.ISessionAggrStatDAO;
import com.mylp.sparkproject.dao.ISessionDetailDAO;
import com.mylp.sparkproject.dao.ISessionRandomExtractDAO;
import com.mylp.sparkproject.dao.ITaskDAO;
import com.mylp.sparkproject.dao.ITop10CategoryDAO;
import com.mylp.sparkproject.dao.ITop10CategorySessionDAO;
import com.mylp.sparkproject.dao.impl.AdBlacklistDAOImpl;
import com.mylp.sparkproject.dao.impl.AdUserClickCountDAOImpl;
import com.mylp.sparkproject.dao.impl.AreaTop3ProductDAOImpl;
import com.mylp.sparkproject.dao.impl.PageSplitConvertRateDAOImpl;
import com.mylp.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.mylp.sparkproject.dao.impl.SessionDetailDAOImpl;
import com.mylp.sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import com.mylp.sparkproject.dao.impl.TaskDAOImpl;
import com.mylp.sparkproject.dao.impl.Top10CategoryDAOImpl;
import com.mylp.sparkproject.dao.impl.Top10CategorySessionDAOImpl;

public class DAOFactory {
	
	/**
	 * 获取任务管理DAO
	 * @return
	 */
	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}
	
	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}

	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}

	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10CategorySessionDAO getTop10CategorySessionDAO() {
		return new Top10CategorySessionDAOImpl();
	}
	
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	
	public static IAreaTop3ProductDAO getaAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}
	
	public static IAdBlacklistDAO getAdBlacklistDAO() {
		return new AdBlacklistDAOImpl();
	}
}
