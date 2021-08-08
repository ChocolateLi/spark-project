package com.chenli.sparkproject.jdbc;

import com.chenli.sparkproject.conf.ConfigurationManager;
import com.chenli.sparkproject.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * jdbc辅助工具
 * <p>
 * 在正式的项目编码编写过程中，是不能出现任何硬编码的字符
 * 比如“张三”、“com.mysql.jdbc.Driver”
 * 所有这些东西，都需要通过常量来封装和使用
 *
 * @author: 小LeetCode~
 **/
public class JDBCHelper {

    //第一步：静态代码块中，直接加载数据库的驱动
    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //第二步，实现JDBCHelper单例化
    //实现单例化的原因就是确保数据库连接池有且仅有一份
    private static JDBCHelper instance = null;

    //数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<>();

    /**
     * 单例化，构造方法私有
     */
    private JDBCHelper() {
        //1.获取数据库连接池大小
        int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        //2.创建指定数量的数据库连接池，并放入数据库连接池中
        for (int i = 0; i < datasourceSize; i++) {
            String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

    }

    /**
     * 获取单例
     */
    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    /**
     * 获取数据库连接的方法
     * 有可能去获取，数据库连接池用光了，暂时获取不到连接
     * 所以需要一个等待机制
     */
    public synchronized Connection getConnection() {
        while (datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 开发增删改查的方法
     * 1、执行增删改SQL语句的方法
     * 2、执行查询SQL语句的方法
     * 3、批量执行SQL语句的方法
     */

    /**
     * 执行增删改SQL语句
     *
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public int executeUpdate(String sql, Object[] params) {
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;


        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }

            rtn = pstmt.executeUpdate();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }

        return rtn;

    }

    /**
     * 执行查询SQL语句
     *
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql, Object[] params,
                             QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstms = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstms = conn.prepareStatement(sql);

            for (int i = 0; i < params.length; i++) {
                pstms.setObject(i + 1, params[i]);
            }

            rs = pstms.executeQuery();

            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 批量执行SQL语句
     * 批量执行SQL语句，是JDBC中的一个高级功能
     * 默认情况下，每次执行一条SQL语句，就会通过网络连接，向MySQL发送一次请求
     *
     * 但是，如果在短时间内要执行多条结构完全一模一样的SQL，只是参数不同
     * 虽然使用PreparedStatement这种方式，可以只编译一次SQL，提高性能，但是，还是对于每次SQL
     * 都要向MySQL发送一次网络请求
     *
     * 可以通过批量执行SQL语句的功能优化这个性能
     * 一次性通过PreparedStatement发送多条SQL语句，比如100条、1000条，甚至上万条
     * 执行的时候，也仅仅编译一次就可以
     * 这种批量执行SQL语句的方式，可以大大提升性能
     * @param sql
     * @param paramsList
     * @return 每天SQL语句的影响的行数
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList) {
        int [] rtn = null;
        Connection conn = null;
        PreparedStatement pstms = null;

        try {
            conn = getConnection();

            //第一步：使用Connection对象，取消自动提交
            conn.setAutoCommit(false);
            pstms = conn.prepareStatement(sql);

            //第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
            for (Object[] params : paramsList) {
                for (int i = 0; i < params.length; i++) {
                    pstms.setObject(i + 1, params[i]);
                }
                pstms.addBatch();
            }

            //第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            rtn = pstms.executeBatch();

            //使用Connetion对象，提交批量的SQL语句
            conn.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return rtn;
    }

    /**
     * 静态你内部类：查询回调接口
     */
    public static interface QueryCallback {

        void process(ResultSet rs) throws Exception;
    }
}
