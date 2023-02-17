package com.sxhs.realtime.util;

import com.sxhs.realtime.bean.*;
import com.sxhs.realtime.common.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

/**
 * @Description: 核酸校验
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 14:14
 */
public class NucCheckUtil {

    /**
     * 检测环节二次校验
     * @param dto
     * @param relationTable
     * @return
     * @throws Exception
     */
    public static Tuple2<List<ProblemDataCr>,List<ProblemDataCr>> reportCheck(ReportDataId dto, Table relationTable) throws Exception {
        ParamCheck check = new ParamCheck();
        List<ProblemDataCr> errorlist = new ArrayList<>();
        List<ProblemDataCr> sucesslist = new ArrayList<>();
        //数据内容格式校验
        String idCard = AESUtil.decrypt(dto.getPersonIdCard());
        String name = AESUtil.decrypt(dto.getPersonName());
        String phone = AESUtil.decrypt(dto.getPersonPhone());
        if (check.checkNotHaveString(idCard) || !check.checkIdCard(idCard)) {
            errorlist.add(setProblemCrFromData(dto, "1", "身份证号不符合标准格式", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "1", "身份证号不符合标准格式", 2, 1));
        }
        if (check.checkNotHaveString(phone) || !check.checkPhone(phone)) {
            errorlist.add(setProblemCrFromData(dto, "3", "电话号码不符合标准格式", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "3", "电话号码不符合标准格式", 2, 1));
        }
        if (check.checkNotHaveString(name)) {
            errorlist.add(setProblemCrFromData(dto, "2", "姓名不符合标准格式", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "2", "姓名不符合标准格式", 2, 1));
        }
        if (StringUtils.isNotBlank(dto.getPackTime()) && dto.getPackTime().compareTo(dto.getCollectTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "打包时间不应早于采样时间", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "打包时间不应早于采样时间", 2, 1));
        }
        if (StringUtils.isNotBlank(dto.getReceiveTime()) && dto.getReceiveTime().compareTo(dto.getCollectTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "接收时间不应早于采样时间", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "接收时间不应早于采样时间", 2, 1));
        }
        if (StringUtils.isNotBlank(dto.getCheckTime()) && dto.getCheckTime().compareTo(dto.getCollectTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "检测时间不应早于采样时间", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "检测时间不应早于采样时间", 2, 1));
        }
        LocalDateTime collect = DateUtils.getDateTimeFromDate(dto.getCollectTime());
        LocalDateTime pack = DateUtils.getDateTimeFromDate(dto.getPackTime());
        LocalDateTime receive = DateUtils.getDateTimeFromDate(dto.getReceiveTime());
        LocalDateTime checkTime = DateUtils.getDateTimeFromDate(dto.getCheckTime());
        if (collect != null && checkTime != null &&Duration.between(collect, checkTime).toMinutes() < 60) {
            errorlist.add(setProblemCrFromData(dto, "6", "检测时间与采样时间间隔低于60分钟，不符合数据标准", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "检测时间与采样时间间隔低于60分钟，不符合数据标准", 2, 1));
        }
        if (collect != null && pack != null && Duration.between(collect, pack).toMinutes() < 2) {
            errorlist.add(setProblemCrFromData(dto, "6", "打包时间与采样时间间隔低于2分钟，不符合数据标准", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "打包时间与采样时间间隔低于2分钟，不符合数据标准", 2, 1));
        }
        if (receive != null && checkTime != null && Duration.between(receive, checkTime).toMinutes() < 20) {
            errorlist.add(setProblemCrFromData(dto, "6", "检测时间与接收时间间隔低于20分钟，不符合数据标准", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "检测时间与接收时间间隔低于20分钟，不符合数据标准", 2, 1));
        }
        if (Objects.nonNull(dto.getPackTime()) && dto.getCheckTime().compareTo(dto.getPackTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "检测时间不应早于打包时间", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "检测时间不应早于打包时间", 2, 1));
        }
        if (Objects.nonNull(dto.getCheckTime()) && dto.getAddTime().compareTo(dto.getCheckTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "上报时间不应早于检测时间", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "上报时间不应早于检测时间", 2, 1));
        }
        if (Objects.nonNull(dto.getReceiveTime()) && dto.getCheckTime().compareTo(dto.getReceiveTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "检测时间不应早于接收时间", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "检测时间不应早于接收时间", 2, 1));
        }
        if ((Objects.isNull(dto.getCollectCount()) && dto.getCollectCount() == 0)
                || Objects.isNull(dto.getCollectLimitnum()) && dto.getCollectLimitnum() == 0) {
            errorlist.add(setProblemCrFromData(dto, "7", "此数据里试管样本数为空或为0", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "7", "此数据里试管样本数为空或为0", 2, 1));
        }
        //数据环节对应校验
        //从hbase拉取数据
        StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
        sj.add(dto.getPersonIdCard());
        sj.add(dto.getTubeCode());
        byte[] rowKey = sj.toString().getBytes();
        Get get = new Get(rowKey);
        Result result = relationTable.get(get);
        String collectTime = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, "collectTime".getBytes()));
        if (StringUtils.isBlank(collectTime)) {
            errorlist.add(setProblemCrFromData(dto, "5", "该检测结果数据无对应的采样数据", 1, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "5", "该检测结果数据无对应的采样数据", 1, 1));
            if (dto.getCollectTime().compareTo(dto.getCheckTime()) > 0) {
                errorlist.add(setProblemCrFromData(dto, "6", "采样时间不应晚于检测时间", 2, 0));
            }else{
                sucesslist.add(setProblemCrFromData(dto, "6", "采样时间不应晚于检测时间", 2, 1));
            }
            if (collect.isBefore(checkTime.minusHours(24))) {
                errorlist.add(setProblemCrFromData(dto, "6", "检测时间与采样时间间隔超24小时", 2, 0));
            }else{
                sucesslist.add(setProblemCrFromData(dto, "6", "检测时间与采样时间间隔超24小时", 2, 1));
            }
        }
        return new Tuple2<>(errorlist,sucesslist);
    }

    /**
     * 接收环节二次校验
     * @param dto
     * @param relationTable
     * @return
     * @throws Exception
     */
    public static Tuple2<List<ProblemDataTre>,List<ProblemDataTre>> receiveCheck(ReceiveDataId dto, Table relationTable) throws Exception {
        List<ProblemDataTre> errorlist = new ArrayList<>();
        List<ProblemDataTre> sucesslist = new ArrayList<>();
        ParamCheck check = new ParamCheck();
        //数据内容格式校验
        //检查格式前先解密
        String deliPhone = AESUtil.decrypt(dto.getDeliveryPrpPhone());
        String deliId = AESUtil.decrypt(dto.getDeliveryPrpId());
        String trPhone = AESUtil.decrypt(dto.getTransportPrpPhone());
        String trId = AESUtil.decrypt(dto.getTransportPrpId());
        if (!check.checkIdCard(deliId)) {
            errorlist.add(createProblemDataTre(dto, "1", "交付人证件号长度有误/含特殊字符，不符合数据标准",2, 0));
        }else{
            sucesslist.add(createProblemDataTre(dto, "1", "交付人证件号长度有误/含特殊字符，不符合数据标准",2, 1));
        }
        if (!check.checkPhone(deliPhone)) {
            errorlist.add(createProblemDataTre(dto, "2", "交付人手机号含特殊字符，不符合数据标准",2, 0));
        }else{
            sucesslist.add(createProblemDataTre(dto, "2", "交付人手机号含特殊字符，不符合数据标准",2, 1));
        }
        if (!check.checkPhone(trPhone)) {
            errorlist.add(createProblemDataTre(dto, "4", "运输人手机号含特殊字符，不符合数据标准",2, 0));
        }else{
            sucesslist.add(createProblemDataTre(dto, "4", "运输人手机号含特殊字符，不符合数据标准",2, 0));
        }
        if (!check.checkIdCard(trId)) {
            errorlist.add(createProblemDataTre(dto, "3", "运输人证件号长度有误/含特殊字符，不符合数据标准",2, 0));
        }else{
            sucesslist.add(createProblemDataTre(dto, "3", "运输人证件号长度有误/含特殊字符，不符合数据标准",2, 1));
        }
        //数据环节对应校验
        //从hbase拉取数据
        byte[] rowKey = dto.getDeliveryCode().getBytes();
        Get get = new Get(rowKey);
        Result result = relationTable.get(get);
        String deliveryTime = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, "deliveryTime".getBytes()));
        String tubeNum = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, "tubeNum".getBytes()));
        String packNum = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, "packNum".getBytes()));
        LocalDateTime receive = DateUtils.getDateTimeFromDate(dto.getReceiveTime());
        if (StringUtils.isNotBlank(deliveryTime) && StringUtils.isNotBlank(tubeNum) && StringUtils.isNotBlank(packNum)) {
            LocalDateTime delivery = DateUtils.getDateTimeFromDate(deliveryTime);
            if (!tubeNum.equals(dto.getTubeNum().intValue())) {
                errorlist.add(createProblemDataTre(dto, "5", "试管数量与转运数据不对应", 2, 0));
            }else{
                sucesslist.add(createProblemDataTre(dto, "5", "试管数量与转运数据不对应", 2, 1));
            }
            if (!packNum.equals(dto.getPackNum().intValue())) {
                errorlist.add(createProblemDataTre(dto, "5", "打包数量与转运数据不对应", 2, 0));
            }else{
                sucesslist.add(createProblemDataTre(dto, "5", "打包数量与转运数据不对应", 2, 1));
            }
            if (delivery.plusHours(24).isBefore(LocalDateTime.now()) && delivery.plusHours(24).isBefore(receive)) {
                errorlist.add(createProblemDataTre(dto, "6", "交付时间和接收时间间隔超过24小时", 2, 0));
            }else{
                sucesslist.add(createProblemDataTre(dto, "6", "交付时间和接收时间间隔超过24小时", 2, 1));
            }
            sucesslist.add(createProblemDataTre(dto, "8", "未发现对应的转运数据", 1, 1));
        }else {
            Duration du = Duration.between(receive, LocalDateTime.now());
            long hours = du.toHours();
            if (StringUtils.isBlank(deliveryTime) && hours >= 24) {
                errorlist.add(createProblemDataTre(dto, "8", "未发现对应的转运数据", 1, 0));
            }
        }
        return new Tuple2<>(errorlist,sucesslist);
    }

    /**
     * 转运环节二次校验
     * @param dto
     * @param relationTable
     * @return
     * @throws IOException
     */
    public static Tuple2<List<ProblemDataTre>,List<ProblemDataTre>> transportCheck(TransportDataId dto, Table relationTable) throws IOException {
        List<ProblemDataTre> errorlist = new ArrayList<>();
        List<ProblemDataTre> sucesslist = new ArrayList<>();
        //数据内容格式校验
        if (dto.getDeliveryTime().compareTo(dto.getPackTime()) < 0) {
            errorlist.add(createProblemDataTre(dto, "6", "此交付单的打包时间早于交付时间，不符合标准", 2, 0));
        }else{
            sucesslist.add(createProblemDataTre(dto, "6", "此交付单的打包时间早于交付时间，不符合标准", 2, 1));
        }
        //数据环节对应校验
        LocalDateTime delivery = DateUtils.getDateTimeFromDate(dto.getDeliveryTime());
        Duration du = Duration.between(delivery, LocalDateTime.now());
        long hours = du.toHours();
        //从hbase拉取数据
        byte[] rowKey = dto.getDeliveryCode().getBytes();
        Get get = new Get(rowKey);
        Result result = relationTable.get(get);
        String receiveTime = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, "receiveTime".getBytes()));
        if (StringUtils.isBlank(receiveTime)) {
            if (hours >= 24) {
                errorlist.add(createProblemDataTre(dto, "7", "此交付单超过24小时未发现接收数据", 1, 0));
            }
        }else{
            LocalDateTime receive = DateUtils.getDateTimeFromDate(receiveTime);
            if (receive.minusHours(24).isAfter(delivery)) {
                errorlist.add(createProblemDataTre(dto, "7", "此交付单超过24小时未发现接收数据", 1, 0));
            }else{
                sucesslist.add(createProblemDataTre(dto, "7", "此交付单超过24小时未发现接收数据", 1, 1));
            }
            for (TransportItem transportItem : dto.getTransportItem()) {
                String tubeCode = transportItem.getTubeCode();
                //从hbase拉取数据
                byte[] tubeRowKey = tubeCode.getBytes();
                Get tubeGet = new Get(tubeRowKey);
                Result tubeResult = relationTable.get(tubeGet);
                String tubeCollectTime = Bytes.toString(tubeResult.getValue(Constants.HBASE_FAMILY, "tubeCollectTime".getBytes()));
                String collectSubmitId = Bytes.toString(tubeResult.getValue(Constants.HBASE_FAMILY, "submitId".getBytes()));
                if (StringUtils.isNotBlank(tubeCollectTime) && StringUtils.isNotBlank(collectSubmitId)) {
                    if (dto.getPackTime().compareTo(tubeCollectTime) < 0) {
                        errorlist.add(createProblemDataTre(dto, "6", "打包时间不应早于采样流水号"+ collectSubmitId + "的采样时间", 2, 0));
                    }else{
                        sucesslist.add(createProblemDataTre(dto, "6", "打包时间不应早于采样流水号"+ collectSubmitId + "的采样时间", 2, 1));
                    }
                    if (dto.getDeliveryTime().compareTo(tubeCollectTime) < 0) {
                        errorlist.add(createProblemDataTre(dto, "6", "交付时间不应早于采样流水号"+ collectSubmitId + "的采样时间", 2, 0));
                    }else{
                        sucesslist.add(createProblemDataTre(dto, "6", "交付时间不应早于采样流水号"+ collectSubmitId + "的采样时间", 2, 1));
                    }
                    LocalDateTime colt = DateUtils.getDateTimeFromDate(tubeCollectTime);
                    if (delivery.minusHours(24).isAfter(colt)) {
                        errorlist.add(createProblemDataTre(dto, "6", "转运单与对应的采样数据的差距时间超过24小时", 2, 0));
                    }else{
                        sucesslist.add(createProblemDataTre(dto, "6", "转运单与对应的采样数据的差距时间超过24小时", 2, 1));
                    }
                    if (hours < 24) {
                        sucesslist.add(createProblemDataTre(dto, "9", "此交付单未发现对应的采样数据", 1, 1));
                    }
                }else{
                    if (hours >= 24) {
                        errorlist.add(createProblemDataTre(dto, "9", "此交付单未发现对应的采样数据", 1, 0));
                    }
                }
            }
        }
        return new Tuple2<>(errorlist,sucesslist);
    }

    /**
     * 采集环节二次校验
     * @param dto
     * @param relationTable
     * @return
     * @throws Exception
     */
    public static Tuple2<List<ProblemDataCr>,List<ProblemDataCr>> collectCheck(CollectDataId dto, Table relationTable) throws Exception {
        ParamCheck check = new ParamCheck();
        List<ProblemDataCr> errorlist = new ArrayList<>();
        List<ProblemDataCr> sucesslist = new ArrayList<>();
        //数据内容格式校验
        //检查格式前先解密
        String name = AESUtil.decrypt(dto.getPersonName());
        String phone = AESUtil.decrypt(dto.getPersonPhone());
        String idCard = AESUtil.decrypt(dto.getPersonIdCard());
        if (StringUtils.isNotEmpty(idCard) && check.checkNotHaveString(idCard)) {
            errorlist.add(setProblemCrFromData(dto, "1", "此数据里证件号有不正规字符", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "1", "此数据里证件号有不正规字符", 2, 1));
        }
        if (StringUtils.isNotEmpty(phone) && check.checkNotHaveString(phone)) {
            errorlist.add(setProblemCrFromData(dto, "3", "此数据里手机号有不正规字符", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "3", "此数据里手机号有不正规字符", 2, 1));
        }
        if ((Objects.isNull(dto.getCollectCount()) && dto.getCollectCount() == 0)
                || Objects.isNull(dto.getCollectLimitnum()) && dto.getCollectLimitnum() == 0) {
            errorlist.add(setProblemCrFromData(dto, "7", "此数据里试管样本数为空或为0", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "7", "此数据里试管样本数为空或为0", 2, 1));
        }
        if (StringUtils.isNotEmpty(name) && check.checkNotHaveString(name)) {
            errorlist.add(setProblemCrFromData(dto, "2", "此数据里人名有不正规字符", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "2", "此数据里人名有不正规字符", 2, 1));
        }
        if (dto.getAddTime().compareTo(dto.getCollectTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "此数据里上报时间不应早于采样时间", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "此数据里上报时间不应早于采样时间", 2, 1));
        }
        //数据环节对应校验
        LocalDateTime collect = DateUtils.getDateTimeFromDate(dto.getCollectTime());
        Duration du = Duration.between(collect, LocalDateTime.now());
        long hours = du.toHours();
        //从hbase拉取数据
        StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
        sj.add(dto.getPersonIdCard());
        sj.add(dto.getTubeCode());
        byte[] rowKey = sj.toString().getBytes();
        Get get = new Get(rowKey);
        Result result = relationTable.get(get);
        String reportTime = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, "checkTime".getBytes()));
        if (hours >= 24) {
            if (StringUtils.isBlank(reportTime)) {
                errorlist.add(setProblemCrFromData(dto, "4", "该采样数据超24小时无对应的检测结果数据", 1, 0));
            } else {
                LocalDateTime checkTime = DateUtils.getDateTimeFromDate(reportTime);
                if (checkTime.minusHours(24).isAfter(collect)) {
                    errorlist.add(setProblemCrFromData(dto, "4", "该采样数据超24小时无对应的检测结果数据",1, 0));
                }else{
                    sucesslist.add(setProblemCrFromData(dto, "4", "该采样数据超24小时无对应的检测结果数据",1, 1));
                }
            }
        }
        return new Tuple2<>(errorlist, sucesslist);
    }

    /**
     * 设置采集工单
     * @param dto
     * @param problemType
     * @param record
     * @param type
     * @return
     */
    public static ProblemDataCr setProblemCrFromData(CollectDataId dto, String problemType, String record, Integer type, Integer isValid) {
        ProblemDataCr data = new ProblemDataCr();
        data.setId(Long.valueOf(SnowflakeIdWorker.generateIdReverse()));
        data.setPerson_name(dto.getPersonName());
        data.setArea_id(dto.getAreaId());
        data.setPerson_id_card(dto.getPersonIdCard());
        data.setSubmit_id(dto.getSubmitId());
        data.setNumber_report(dto.getNumberReport());
        data.setCollect_time(dto.getCollectTime());
        data.setSource(1);
        data.setTYPE(type);
        data.setProblem_type(problemType);
        data.setCreate_time(Constants.FASTDATEFORMAT.format(new Date()));
        data.setAdd_time(StringUtils.isBlank(dto.getAddTime()) || "null".equalsIgnoreCase(dto.getAddTime()) ? Constants.FASTDATEFORMAT.format(new Date()): dto.getAddTime());
        data.setProblem_record(record);
        data.setIs_valid(isValid);
        data.setIs_delete(0);
        data.setCreate_by(dto.getUserName());
        data.setUpdate_by(dto.getUserName());
        data.setUpdate_time(Constants.FASTDATEFORMAT.format(new Date()));
        return data;
    }

    /**
     * 设置检测工单
     * @param dto
     * @param problemType
     * @param record
     * @param type
     * @return
     */
    public static ProblemDataCr setProblemCrFromData(ReportDataId dto, String problemType, String record, Integer type, Integer isValid) {
        ProblemDataCr data = new ProblemDataCr();
        data.setId(Long.valueOf(SnowflakeIdWorker.generateIdReverse()));
        data.setPerson_name(dto.getPersonName());
        data.setArea_id(dto.getAreaId());
        data.setPerson_id_card(dto.getPersonIdCard());
        data.setSubmit_id(dto.getSubmitId());
        data.setCollect_time(dto.getCollectTime());
        data.setNumber_report(dto.getNumberReport());
        data.setCheck_time(dto.getCheckTime());
        data.setSource(4);
        data.setTYPE(type);
        data.setProblem_type(problemType);
        data.setCreate_time(Constants.FASTDATEFORMAT.format(new Date()));
        data.setAdd_time(StringUtils.isBlank(dto.getAddTime()) || "null".equalsIgnoreCase(dto.getAddTime()) ? Constants.FASTDATEFORMAT.format(new Date()): dto.getAddTime());
        data.setProblem_record(record);
        data.setIs_valid(isValid);
        data.setIs_delete(0);
        data.setCreate_by(dto.getUserName());
        data.setUpdate_by(dto.getUserName());
        data.setUpdate_time(Constants.FASTDATEFORMAT.format(new Date()));
        return data;
    }

    /**
     * 设置转运工单
     * @param dto
     * @param problemType
     * @param record
     * @param type
     * @return
     */
    public static ProblemDataTre createProblemDataTre(TransportDataId dto, String problemType, String record, int type, Integer isValid) {
        ProblemDataTre data = new ProblemDataTre();
        data.setId(Long.valueOf(SnowflakeIdWorker.generateIdReverse()));
        data.setSource(2);
        data.setSubmit_id(dto.getSubmitId());
        data.setCreate_time(Constants.FASTDATEFORMAT.format(new Date()));
        data.setArea_id(dto.getAreaId());
        data.setAdd_time(StringUtils.isBlank(dto.getAddTime()) || "null".equalsIgnoreCase(dto.getAddTime()) ? Constants.FASTDATEFORMAT.format(new Date()): dto.getAddTime());
        data.setCreate_by(dto.getUserName());
        data.setName(dto.getDeliveryName());
        data.setCode(dto.getDeliveryCode());
        data.setTime(dto.getDeliveryTime());
        data.setIs_valid(isValid);
        data.setNumber_report(Long.valueOf(dto.getNumberReport()));
        data.setProblem_record(record);
        data.setTYPE(type);
        data.setProblem_type(problemType);
        data.setIs_delete(0);
        data.setUpdate_by(dto.getUserName());
        data.setUpdate_time(Constants.FASTDATEFORMAT.format(new Date()));
        return data;
    }

    /**
     * 设置接收工单
     * @param dto
     * @param problemType
     * @param record
     * @param type
     * @return
     */
    public static ProblemDataTre createProblemDataTre(ReceiveDataId dto, String problemType, String record, int type, Integer isValid) {
        ProblemDataTre data = new ProblemDataTre();
        data.setId(Long.valueOf(SnowflakeIdWorker.generateIdReverse()));
        data.setSource(3);
        data.setArea_id(dto.getAreaId());
        data.setSubmit_id(dto.getSubmitId());
        data.setCreate_time(Constants.FASTDATEFORMAT.format(new Date()));
        data.setAdd_time(StringUtils.isBlank(dto.getAddTime()) || "null".equalsIgnoreCase(dto.getAddTime()) ? Constants.FASTDATEFORMAT.format(new Date()): dto.getAddTime());
        data.setCreate_by(dto.getUserName());
        data.setName(dto.getReceiveName());
        data.setCode(dto.getReceiveCode());
        data.setTime(dto.getReceiveTime());
        data.setIs_valid(isValid);
        data.setNumber_report(Long.valueOf(dto.getNumberReport()));
        data.setProblem_record(record);
        data.setTYPE(type);
        data.setProblem_type(problemType);
        data.setIs_delete(0);
        data.setUpdate_by(dto.getUserName());
        data.setUpdate_time(Constants.FASTDATEFORMAT.format(new Date()));
        return data;
    }
}
