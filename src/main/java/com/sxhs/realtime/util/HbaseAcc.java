package com.sxhs.realtime.util;

import lombok.Data;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.List;

@Data
public class HbaseAcc {
    private List<String> list = new ArrayList<>();
    private int batch = 50;

    public void addElement(String str) {
        list.add(str);
        // todo 大于batch查询hbase并缓存结果
    }

    public Integer getValue() {
        int result = 0;
        if (list.size() > 0) {
            List<Get> batchList = new ArrayList<>();
            for (String str : list) {
                batchList.add(new Get(str.getBytes()));
                if (batchList.size() >= batch) {
                    result += _doSearch(batchList);
                }
            }
            if (batchList.size() > 0) {
                result += _doSearch(batchList);
            }
        }
        return result;
    }

    private int _doSearch(List<Get> batchList) {
        Result[] results = HbaseUtil.searchHbase(batchList);
        int count = 0;
        if (results != null && results.length > 0) {
            for (Result r : results) {
                if (!r.isEmpty()) {
                    count++;
                }
            }
        }
        return count;
    }
}
