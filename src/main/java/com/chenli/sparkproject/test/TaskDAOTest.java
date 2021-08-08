package com.chenli.sparkproject.test;

import com.chenli.sparkproject.dao.ITaskDAO;
import com.chenli.sparkproject.domain.Task;
import com.chenli.sparkproject.impl.DAOFactory;

/**
 * 任务管理类测试
 * @author: 小LeetCode~
 **/
public class TaskDAOTest {

    public static void main(String[] args) {
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1);
        System.out.println(task.getTaskName());
    }
}
