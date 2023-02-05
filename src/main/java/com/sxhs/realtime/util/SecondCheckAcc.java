package com.sxhs.realtime.util;

import com.sxhs.realtime.bean.ProblemDataCr;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class SecondCheckAcc<T> {
    private static final String TYPE1 = "1";
    private static final String TYPE2 = "2";
    private static final String CONNECT = ",";

    private List<T> dataList = new ArrayList<>();

    public void add(T collectDataId) {
        dataList.add(collectDataId);
    }

    public String getResult(List<List<ProblemDataCr>> list) {
        int all = 0;
        int type1 = 0;
        int type2 = 0;
        for (List<ProblemDataCr> errList : list) {
            boolean error1 = false;
            boolean error2 = false;
            if (errList != null && errList.size() > 0) {
                all++;
                for (ProblemDataCr pdc : errList) {
                    if (TYPE1.equals(pdc.getProblem_type()) && !error1) {
                        error1 = true;
                    } else if (TYPE2.equals(pdc.getProblem_type()) && !error2) {
                        error2 = true;
                    }
                }
                if (error1) {
                    type1++;
                }
                if (error2) {
                    type2++;
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        return sb.append(all).append(CONNECT).append(type1).append(CONNECT).append(type2).toString();
    }
}
