package com.chenli.sparkproject.test;

import com.chenli.sparkproject.conf.ConfigurationManager;

/**
 * @author: 小LeetCode~
 **/
public class ConfigurationManagerTest {

    public static void main(String[] args) {
        String testkey1 = ConfigurationManager.getProperty("testkey1");
        String testkey2 = ConfigurationManager.getProperty("testkey2");
        System.out.println(testkey1);
        System.out.println(testkey2);
    }
}
