package com.ltz.spark.config;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理
 */
public class ConfigurationManager {

    private static Properties prop = new Properties();

    static {

        try {
            InputStream in = ConfigurationManager.class.
                    getClassLoader().getResourceAsStream("my.properties");

            prop.load(in);
            in.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String get(String key){

        return prop.getProperty(key);
    }
    public static void set(String key,String value){
        prop.setProperty(key,value);
    }
}
