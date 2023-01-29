package com.sxhs.realtime.window;

import com.sxhs.realtime.util.HbaseUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class HbaseTest {


    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        list.add("person_id_card_tube_code");
        list.add("test1");
        list.add("test2");
        list.add("test3");
        List<Get> getList = list.stream().map(str -> new Get(str.getBytes())).collect(Collectors.toList());

        Result[] results = HbaseUtil.searchHbase(getList);
        System.out.println("results length: " + results.length);
        AtomicInteger count = new AtomicInteger();
        Arrays.stream(results).forEach(result -> {
            if (!result.isEmpty())
                count.getAndIncrement();
        });
        System.out.println(count.get());
    }
}
