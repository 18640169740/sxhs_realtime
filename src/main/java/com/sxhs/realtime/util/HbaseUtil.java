package com.sxhs.realtime.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// TODO 单例/关闭连接
public class HbaseUtil {
    private static final Logger logger = LoggerFactory.getLogger(HbaseUtil.class);
    private static Connection conn = null;

    static {
        try {
            conn = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Result[] searchHbase(List<Get> getList) {
        Table table = null;
        Result[] result = null;
        if (getList == null || getList.size() == 0) {
            return null;
        }
        try {
            table = conn.getTable(TableName.valueOf("default", "nuc_relation_distinct"));
            result = table.get(getList);
        } catch (IOException e) {
            logger.error("hbase search failed");
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException ignored) {
                }
            }
        }
        return result;
    }

    public static void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException ignored) {
            }
        }
    }

//    public static void searchAll() throws IOException {
//        Table table = conn.getTable(TableName.valueOf("caches", "nuc_collect_distinct"));
//        //得到用于扫描region的对象
//        Scan scan = new Scan();
//        //使用HTable得到resultcanner实现类的对象
//        ResultScanner resultScanner = table.getScanner(scan);
//        for (Result result : resultScanner) {
//            Cell[] cells = result.rawCells();
//            for (Cell cell : cells) {
//                //得到rowkey
//                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
//                //得到列族
//                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
//                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
//                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
//            }
//
//        }
//
//
//    }
//
//    public static void main(String[] args) throws IOException {
//        List<Get> list = new ArrayList<>();
//        list.add(new Get("person_id_card_tube_code".getBytes()));
//        list.add(new Get("person_id_card_tube_code".getBytes()));
//
//        Result[] results = HbaseUtil.searchHbase(list);
//        int count = 0;
//        for (Result result : results) {
//            if (!result.isEmpty()) {
//                count++;
//            }
//        }
//        System.out.println(count);
//        HbaseUtil.closeConnection();
//    }
}
