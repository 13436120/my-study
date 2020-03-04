/**
 * 
 */
package com.zking.shopping;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 获得数据库连接的工具类
 * 
 * @author hb
 *
 * @date 2016年8月25日 上午9:22:10
 */
public class ConnectionUtils {
	private static String url;
	private static String username;
	private static String classDriver;
	private static String pwd;
	static {
		try {
			url = "jdbc:mysql://10.1.23.10:6033/uw_audit_cold_sit?useUnicode=true&characterEncoding=utf-8";
			username = "uw_audit_cold_sit";
			pwd = "auditcold2019";
			classDriver = "com.mysql.jdbc.Driver";
			// 驱动加载
			Class.forName(classDriver);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/*********************
	 * 在业务层中设置手动管理事务提供的属性.业务层都需要从TreahdLocal中获得数据
	 */
	private static ThreadLocal<Connection> tl = new ThreadLocal<Connection>();

	/**
	 * 释放资源的方法
	 * 
	 * @param conn
	 * @param pstm
	 * @param rs
	 */
	public static void colse(Connection conn, Statement pstm, ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
			if (pstm != null) {
				pstm.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			throw new DaoException(e);
		}
	}

	/** 获得一个新的Connection对象
	 * @return */
	public static Connection newConnection() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, username, pwd);
		} catch (Exception e) {
			throw new DaoException(e);
		}
		return conn;
	}

	/** 回滚事务
	 * @param conn */
	public static void rollback(Connection conn) {
		try {
			if (conn != null)
				conn.rollback();
		} catch (Exception e) {
			throw new DaoException(e);
		}
	}

	/**
	 * 提交事务
	 * 
	 * @param conn
	 */
	public static void commit(Connection conn) {
		try {
			/******************
			 * Connection不为空,并且Connection没有关闭
			 */
			if (conn != null && !conn.isClosed())
				conn.commit();
		} catch (Exception e) {
			throw new DaoException(e);
		}
	}

	/**
	 * 从线程的局部变量中获得Connection对象
	 * 
	 * @return
	 */
	public static Connection getConnection4TL() throws DaoException {
		Connection conn = tl.get();
		try {
			if (conn == null || conn.isClosed()) {
				// 需要获得一个新的Conneciton;
				conn = newConnection();
				// 将Connection绑定到ThreadLocal上面
				tl.set(conn);
			}

			return conn;
		} catch (Exception e) {
			throw new DaoException(e);
		}

	}

	public static void main(String[] args) {
		System.out.println(newConnection());
	}
}
