package com.jaslou.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import com.jaslou.domin.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * send the data to mysql
 */
public class MysqlRichSinkFunction extends RichSinkFunction<List<UserBehavior>> {

    Logger logger = LoggerFactory.getLogger(FlinkSinkToMysqlMain.class);

    private DruidDataSource dataSource;
    private PreparedStatement pstm;
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://192.168.244.10:3306/mytest");
        dataSource.setUsername("root");
        dataSource.setPassword("111111");
        dataSource.setMaxActive(20);
        dataSource.setMinIdle(10);
        dataSource.setInitialSize(5);
        String sql = "insert into tb_user_behavior(user_id, item_id, category_id,behavior_type,create_time) values(?,?,?,?,?);";
        try{
            conn = dataSource.getConnection();
            logger.info("##### create datasource connection");
        }catch(Exception e){
            logger.error("##### mysql get connection has exception , msg = {}", e.getMessage());
        }
        if(conn != null){
            pstm = conn.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(pstm != null){
            pstm.close();
        }
        if(conn != null){
            conn.close();
        }
    }

    @Override
    public void invoke(List<UserBehavior> userBehaviorList, Context context) throws Exception {
        for(UserBehavior userBehavior: userBehaviorList){
            pstm.setLong(1, userBehavior.userId);
            pstm.setLong(2, userBehavior.itemId);
            pstm.setInt(3, userBehavior.categoryId);
            pstm.setString(4, userBehavior.behavior);
            pstm.setLong(5, userBehavior.timestamp);
            pstm.addBatch();
        }
        int[] insertCount = pstm.executeBatch();
        logger.info("成功插入了{}条数据", insertCount.length);
    }
}
