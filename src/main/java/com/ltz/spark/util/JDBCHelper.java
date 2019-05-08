package com.ltz.spark.util;

import com.ltz.spark.Constants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCHelper {

    static {
        try {
            Class.forName(Constants.JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(){
        try {
            return  DriverManager
                    .getConnection(Constants.JDBC_URL,Constants.USER,Constants.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }



}
