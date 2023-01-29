//package com.sxhs.realtime.util;
//
//import org.apache.flink.table.functions.AggregateFunction;
//
//public class ProblemNumberStatUdf extends AggregateFunction<Integer, TmpType> {
//
//    public void accumulate(TmpType acc, String type, String personIdCard, String personPhone, String personName,
//                           Integer collectCount, Integer collectLimitnum, String addTime, String collectTime,
//                           String packTime, String receiveTime, String checkTime) {
//        boolean result = true;
//        switch (type) {
//            case "1":
//                result = CheckUtil.checkIdPhoneName(personIdCard, personPhone, personName)
//                        && CheckUtil.checkCollectTime(collectTime, addTime)
//                        && CheckUtil.checkCollectCount(collectCount, collectLimitnum);
//                break;
//            case "2":
//                break;
//            case "3":
//                break;
//            case "4":
//                break;
//
//        }
//        if (!result) {
//            acc.setCount(acc.getCount() + 1);
//        }
//    }
//
//
//    @Override
//    public Integer getValue(TmpType accumulator) {
//        return accumulator.getCount();
//    }
//
//    @Override
//    public TmpType createAccumulator() {
//        return new TmpType();
//    }
//}
