package com.chenli.sparkproject.test;

/**
 * 单例模式
 *
 * @author: 小LeetCode~
 **/
public class Singleton {

    private static Singleton instance = null;

    //构造方法私有化
    private Singleton(){}

    public static Singleton getInstance(){
        //两步检查机制
        if (instance == null) {
            synchronized (Singleton.class){
                if (instance == null) {
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }


}
