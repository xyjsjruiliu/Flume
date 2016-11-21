package com.xy.lr.java.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * hbase sink
 * Created by hadoop on 11/15/16.
 */
public class HBaseSink extends AbstractSink implements Configurable {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    //hbase 表名
    private String hbaseTableName;
    //hbase 列族名
    private String familyName;

    //hbase 配置文件路径
    private String hbase_conf_path;
    //hadoop 配置文件路径
    private String hadoop_conf_path;

    @Override
    public void start() {
        configuration = HBaseConfiguration.create();

        configuration.addResource(hbase_conf_path);
        configuration.addResource(hadoop_conf_path);

        try {
            //连接HBase
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

        super.start();
    }

    /**
     * 关闭连接
     */
    @Override
    public void stop() {
        close();
    }

    /**
     * 关闭连接
     */
    public static  void close(){
        try {
            if(null != admin)
                admin.close();
            if(null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 处理程序
     * @return 处理结果
     * @throws EventDeliveryException
     */
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Transaction tx = null;
        try {
            Channel channel = getChannel();
            tx = channel.getTransaction();
            tx.begin();
            for (int i = 0; i < 100; i++) {
                Event event = channel.take();
                if (event == null) {
                    status = Status.BACKOFF;
                    break;
                } else {
                    String body = new String(event.getBody());
                    System.out.println(body);
                }
            }
            tx.commit();
        } catch (Exception e) {
            if (tx != null) {
                tx.commit();
            }
            e.printStackTrace();
        } finally {
            if (tx != null) {
                tx.close();
            }
        }
        return status;

    }

    /**
     * 从配置文件中得到配置信息
     * @param context 配置
     */
    public void configure(Context context) {
        this.hadoop_conf_path = context.getString(HBaseSinkConfigurationConstants.HADOOP_CONF_PATH);
        this.hbase_conf_path = context.getString(HBaseSinkConfigurationConstants.HBASE_CONF_PATH);
        this.hbaseTableName = context.getString(HBaseSinkConfigurationConstants.CONFIG_TABLE);
        this.familyName = context.getString(HBaseSinkConfigurationConstants.CONFIG_COLUMN_FAMILY);
    }

    /**
     *
     * @throws IOException
     */
    public void createTable() throws IOException {
        //列族是用,分割
        String[] cols = this.familyName.split(",");

        TableName tableNames = TableName.valueOf(this.hbaseTableName);

        if(admin.tableExists(tableNames)){
            TableName tn = TableName.valueOf(this.hbaseTableName);
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
            }
            System.out.println("talbe is re-create!");
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(this.hbaseTableName);
        for(String col:cols){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
            //添加列族
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //创建表
        admin.createTable(hTableDescriptor);
    }
}
