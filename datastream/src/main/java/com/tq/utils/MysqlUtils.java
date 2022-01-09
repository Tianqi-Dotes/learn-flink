package com.tq.utils;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlUtils {

    public static Connection getMysqlConnection(){
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection("jdbc:mysql://localhost:3306/flink","root","root");
        }catch (Exception e){
            return null;
        }

    }

    public static void close(Connection c, PreparedStatement ps) throws SQLException {
        if(c!=null){
            c.close();
        }
        if(ps!=null){
            ps.close();
        }
    }


}
