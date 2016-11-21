package com.xy.lr.java.hbase;

import com.xy.lr.java.spider.SpiderConfiguration;
import com.xy.lr.java.spider.SpiderXMLParser;
import com.xy.lr.java.spider.entity.SpiderAuthor;
import com.xy.lr.java.tools.file.FindAllFileOnCatalogue;
import com.xy.lr.java.tools.file.JFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hadoop on 11/14/16.
 */
public class Temp_HBase {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    /**
     * 初始化链接
     */
    public static void init(){
        configuration = HBaseConfiguration.create();

        /*configuration.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        configuration.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        */configuration.addResource("hbase-site.xml");
        configuration.addResource("core-site.xml");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
     *
     * @param tableName 表名
     * @param cols 列族名
     * @throws IOException
     */
    public static void createTable(String tableName,String[] cols) throws IOException {

        init();
        TableName tableNames = TableName.valueOf(tableName);

        if(admin.tableExists(tableNames)){
            TableName tn = TableName.valueOf(tableName);
            if (admin.tableExists(tn)) {
                admin.disableTable(tn);
                admin.deleteTable(tn);
            }
            System.out.println("talbe is re-create!");
        }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String col:cols){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                //添加列族
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            //创建表
            admin.createTable(hTableDescriptor);

//        close();
    }

    /**
     *
     * @param filePath
     * @return
     */
    public static List<File> getFileList(String filePath) {
        FindAllFileOnCatalogue findAllFileOnCatalogue = new FindAllFileOnCatalogue();

        List<File> list = findAllFileOnCatalogue.getCatalogueList(new File(filePath));
        return list;
    }

    /**
     * 插入数据
     * @param tableName 表名
     * @param rowkey 行健
     * @param colFamily 列族名
     * @param col 列
     * @param val 值
     * @throws IOException
     */
    public static void insterRow(String tableName, String rowkey,
                                 String colFamily, String col,String val) throws IOException {
//        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        //批量插入
       /* List<Put> putList = new ArrayList<Put>();
        puts.add(put);
        table.put(putList);*/
        table.close();
        close();
    }

    /**
     *
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean insterRows(String tableName, List<Put> puts) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            return false;
        }
        try {
            table.put(puts);
        } catch (IOException e) {
            return false;
        }
        try {
            table.close();
        } catch (IOException e) {
            return false;
        }
        close();
        return true;
    }

    public static void main(String[] args) throws IOException {
        SpiderXMLParser spiderXMLParser = new SpiderXMLParser();
        List<File> list = getFileList(args[0]);

        //xml 文件列表
        List<SpiderConfiguration> confs = new ArrayList<SpiderConfiguration>();

        try {
            createTable("Paper", new String[]{"inf", "authors"});
        } catch (IOException e) {
            System.out.println("create hbase table error!");
        }

        if(new File("xmlParsed.xml").exists()) {
            new File("xmlParsed.xml").delete();
        }

        List<Put> puts = new ArrayList<Put>();
        for(File file : list) {
            JFile.appendFile("xmlParsed.xml","------------------------------------------------");
            JFile.appendFile("xmlParsed.xml", file.getName());
            //解析xml文件
            SpiderConfiguration conf = spiderXMLParser.parser(file.getAbsolutePath());
            if(conf != null){//处理正确的
//                confs.add(conf);

                //行健
                Put put = new Put(Bytes.toBytes(conf.getTitle()));
                put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes("time"),
                        Bytes.toBytes(conf.getTime()));
                put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes("sortnumber"),
                        Bytes.toBytes(conf.getSortNumber()));
                put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes("fundsproject"),
                        Bytes.toBytes(conf.getFundsProject()));
                put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes("keywords"),
                        Bytes.toBytes(conf.getKeyWords()));
                put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes("abstracts"),
                        Bytes.toBytes(conf.getAbstracts()));
                put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes("indexs"),
                        Bytes.toBytes(conf.getIndex()));
                put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes("publishinghouse"),
                        Bytes.toBytes(conf.getPublishingHouse()));

                List<SpiderAuthor> spiderAuthors = conf.getSpiderAuthors();
                for(int i = 0;i < spiderAuthors.size() ;i++){
                    SpiderAuthor spiderAuthor = spiderAuthors.get(i);
                    /*System.out.println(spiderAuthor.getcName());
                    System.out.println(spiderAuthor.geteName());
                    System.out.println(spiderAuthor.getOrgization());
                    */
                    JFile.appendFile("xmlParsed.xml",
                            spiderAuthor.getcName() +
                                    spiderAuthor.getOrgization() +
                            spiderAuthor.geteName());
                    put.addColumn(
                            Bytes.toBytes("authors"),
                            Bytes.toBytes(spiderAuthor.getcName() + "_" + spiderAuthor.geteName() + "," +(i+1)),
                            Bytes.toBytes(spiderAuthor.getOrgization()));
                }

                puts.add(put);
            }
        }
        insterRows("Paper", puts);

/*
        getData("Paper",
                "脉冲中子地层元素测井仪优化设计中的蒙特卡罗模拟研究  Study of Monte Carlo Simulation on Optimally Designing Pulsing Neutron Formation Element Logging Tool",
                "authors", "寅新艺");
*/
    }

    //根据rowkey查找数据
    public static void getData(String tableName,String rowkey,String colFamily,String col)throws  IOException{
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        //获取指定列族数据
        //get.addFamily(Bytes.toBytes(colFamily));
        //获取指定列数据
        //get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        Result result = table.get(get);

        showCell(result);
        table.close();
        close();
    }

    //格式化输出
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
    }
}
