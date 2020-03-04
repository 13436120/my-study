package com.zking.shopping;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Statement;
import java.text.MessageFormat;

/**
 * Hello world!
 *
 */
public class AppRunningForProgress2
{
    
    private  static  String pathname = "D:\\sql\\t_uw\\";


    private static String origTableName = "t_uw_progress";
    
    private static String createTableSql = 
            "CREATE TABLE IF NOT EXISTS `{0}` like "+origTableName;

    private static  long insertNum=0;
    
    public static void main( String[] args ) throws Exception {
       
        File file = new File(pathname);
        String[] list = file.list();
        int startFile=201508;
        for (int i = 0; i < list.length; i++) {
            String fileName = list[i];

            String fileNamePre = "t_uw_progress_";
            String fileNamePost = ".sql";
            String tableNameNumPart = fileName.substring(fileName.indexOf(fileNamePre)+fileNamePre.length(),fileName.indexOf(fileNamePost));

            if (!fileName.startsWith(fileNamePre)) {
                continue;
            }

            if(Integer.valueOf(tableNameNumPart)<startFile){
                continue;
            }

            tableNameNumPart = origTableName + "_" + tableNameNumPart;
            createTable(tableNameNumPart);
            truncateTable(tableNameNumPart);
            inserData(fileName,tableNameNumPart);
        }
        

    }


    public static void createTable(String tableName){
        Connection conn = null;
        Statement stm = null;
        try {
            conn = ConnectionUtils.getConnection4TL();
            stm = conn.createStatement();
            stm.executeUpdate(MessageFormat.format(createTableSql,tableName));
            System.out.println("表："+tableName+"创建成功");
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException();
        }finally {
            ConnectionUtils.colse(conn, stm, null);
        }
    }


    public static void truncateTable(String tableName){
        Connection conn = null;
        Statement stm = null;
        try {
            conn = ConnectionUtils.getConnection4TL();
            stm = conn.createStatement();
            stm.executeUpdate("truncate table "+tableName);
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException();
        }finally {
            ConnectionUtils.colse(conn, stm, null);
        }
    }




    public static void inserData(String fileName,String tableName) throws Exception {
        // 1.获得Connection对象
        Connection conn = null;
        String insertSqlPre = "INSERT INTO " + tableName + " ( last_node_status,create_time,flow_time,task_id,sequnce_num,valid,task_status,flow_status,last_user,last_node,flow_id,last_time,create_user ) VALUES ";

        StringBuilder appendedSql = new StringBuilder();
        String splitStr = "VALUES";
        FileInputStream fis=null;
        InputStreamReader isr=null;
        BufferedReader br=null;
        Statement statement=null;
        try {
            conn = ConnectionUtils.getConnection4TL();
            conn.setAutoCommit(false);
            fis = new FileInputStream(pathname+fileName);
            isr = new InputStreamReader(fis, "UTF-8");
            br= new BufferedReader(isr);
            statement=conn.createStatement();
            String line = "";
            int maxNum=1000;
            int maxBatch=10;
            int i=1;
            int j=1;
            boolean hasNoExecuteBatch=false;//是否还有未执行完的sql

            long startTime = System.currentTimeMillis();
            insertNum=0;
            while ((line = br.readLine()) != null) {

                insertNum++;

                int startIndex = line.indexOf(splitStr)+splitStr.length();
                appendedSql.append(line.substring(startIndex, line.length() - 1));//去掉最后一个分号
                appendedSql.append(",");

                if(j>maxBatch){
                    statement.executeBatch();
                    conn.commit();
                    j=1;
                    hasNoExecuteBatch=false;

                    startTime = logInsertCostTime(startTime);

                }
                if (i==maxNum){
                    appendedSql = new StringBuilder(appendedSql.substring(0, appendedSql.length()-1));//去掉最后的逗号
                    statement.addBatch(insertSqlPre+appendedSql.toString());
                    j++;
                    appendedSql = new StringBuilder();
                    i=0;
                    hasNoExecuteBatch=true;
                }
                i++;

            }
            if (appendedSql.length()>0) {
                appendedSql = new StringBuilder(appendedSql.substring(0, appendedSql.length()-1));//去掉最后的逗号
                statement.addBatch(insertSqlPre+appendedSql.toString());
                hasNoExecuteBatch=true;
            }
            if (hasNoExecuteBatch) {
                statement.executeBatch();
                conn.commit();

                logInsertCostTime(startTime);
            }


        }  finally {
            br.close();
            isr.close();
            fis.close();
            ConnectionUtils.colse(conn, statement, null);
        }

    }

    private static long logInsertCostTime(long startTime) {
        long endTime = System.currentTimeMillis();
        System.out.println("处理"+insertNum+"条用时毫秒："+(endTime-startTime));
        startTime=System.currentTimeMillis();
        insertNum=0;
        return startTime;
    }
}
