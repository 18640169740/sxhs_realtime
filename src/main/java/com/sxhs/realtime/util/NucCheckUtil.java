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
 * @Description: ����У��
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/30 14:14
 */
public class NucCheckUtil {

    /**
     * ��⻷�ڶ���У��
     * @param dto
     * @param relationTable
     * @return
     * @throws Exception
     */
    public static Tuple2<List<ProblemDataCr>,List<ProblemDataCr>> reportCheck(ReportDataId dto, Table relationTable) throws Exception {
        ParamCheck check = new ParamCheck();
        List<ProblemDataCr> errorlist = new ArrayList<>();
        List<ProblemDataCr> sucesslist = new ArrayList<>();
        //�������ݸ�ʽУ��
        String idCard = AESUtil.decrypt(dto.getPersonIdCard());
        String name = AESUtil.decrypt(dto.getPersonName());
        String phone = AESUtil.decrypt(dto.getPersonPhone());
        if (check.checkNotHaveString(idCard) || !check.checkIdCard(idCard)) {
            errorlist.add(setProblemCrFromData(dto, "1", "���֤�Ų����ϱ�׼��ʽ", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "1", "���֤�Ų����ϱ�׼��ʽ", 2, 1));
        }
        if (check.checkNotHaveString(phone) || !check.checkPhone(phone)) {
            errorlist.add(setProblemCrFromData(dto, "3", "�绰���벻���ϱ�׼��ʽ", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "3", "�绰���벻���ϱ�׼��ʽ", 2, 1));
        }
        if (check.checkNotHaveString(name)) {
            errorlist.add(setProblemCrFromData(dto, "2", "���������ϱ�׼��ʽ", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "2", "���������ϱ�׼��ʽ", 2, 1));
        }
        if (StringUtils.isNotBlank(dto.getPackTime()) && dto.getPackTime().compareTo(dto.getCollectTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "���ʱ�䲻Ӧ���ڲ���ʱ��", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "���ʱ�䲻Ӧ���ڲ���ʱ��", 2, 1));
        }
        if (StringUtils.isNotBlank(dto.getReceiveTime()) && dto.getReceiveTime().compareTo(dto.getCollectTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "����ʱ�䲻Ӧ���ڲ���ʱ��", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "����ʱ�䲻Ӧ���ڲ���ʱ��", 2, 1));
        }
        if (StringUtils.isNotBlank(dto.getCheckTime()) && dto.getCheckTime().compareTo(dto.getCollectTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "���ʱ�䲻Ӧ���ڲ���ʱ��", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "���ʱ�䲻Ӧ���ڲ���ʱ��", 2, 1));
        }
        LocalDateTime collect = DateUtils.getDateTimeFromDate(dto.getCollectTime());
        LocalDateTime pack = DateUtils.getDateTimeFromDate(dto.getPackTime());
        LocalDateTime receive = DateUtils.getDateTimeFromDate(dto.getReceiveTime());
        LocalDateTime checkTime = DateUtils.getDateTimeFromDate(dto.getCheckTime());
        if (collect != null && checkTime != null &&Duration.between(collect, checkTime).toMinutes() < 60) {
            errorlist.add(setProblemCrFromData(dto, "6", "���ʱ�������ʱ��������60���ӣ����������ݱ�׼", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "���ʱ�������ʱ��������60���ӣ����������ݱ�׼", 2, 1));
        }
        if (collect != null && pack != null && Duration.between(collect, pack).toMinutes() < 2) {
            errorlist.add(setProblemCrFromData(dto, "6", "���ʱ�������ʱ��������2���ӣ����������ݱ�׼", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "���ʱ�������ʱ��������2���ӣ����������ݱ�׼", 2, 1));
        }
        if (receive != null && checkTime != null && Duration.between(receive, checkTime).toMinutes() < 20) {
            errorlist.add(setProblemCrFromData(dto, "6", "���ʱ�������ʱ��������20���ӣ����������ݱ�׼", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "���ʱ�������ʱ��������20���ӣ����������ݱ�׼", 2, 1));
        }
        if (Objects.nonNull(dto.getPackTime()) && dto.getCheckTime().compareTo(dto.getPackTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "���ʱ�䲻Ӧ���ڴ��ʱ��", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "���ʱ�䲻Ӧ���ڴ��ʱ��", 2, 1));
        }
        if (Objects.nonNull(dto.getCheckTime()) && dto.getAddTime().compareTo(dto.getCheckTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "�ϱ�ʱ�䲻Ӧ���ڼ��ʱ��", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "�ϱ�ʱ�䲻Ӧ���ڼ��ʱ��", 2, 1));
        }
        if (Objects.nonNull(dto.getReceiveTime()) && dto.getCheckTime().compareTo(dto.getReceiveTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "���ʱ�䲻Ӧ���ڽ���ʱ��", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "���ʱ�䲻Ӧ���ڽ���ʱ��", 2, 1));
        }
        if ((Objects.isNull(dto.getCollectCount()) && dto.getCollectCount() == 0)
                || Objects.isNull(dto.getCollectLimitnum()) && dto.getCollectLimitnum() == 0) {
            errorlist.add(setProblemCrFromData(dto, "7", "���������Թ�������Ϊ�ջ�Ϊ0", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "7", "���������Թ�������Ϊ�ջ�Ϊ0", 2, 1));
        }
        //���ݻ��ڶ�ӦУ��
        //��hbase��ȡ����
        StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
        sj.add(dto.getPersonIdCard());
        sj.add(dto.getTubeCode());
        byte[] rowKey = sj.toString().getBytes();
        Get get = new Get(rowKey);
        Result result = relationTable.get(get);
        String collectTime = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, "collectTime".getBytes()));
        if (StringUtils.isBlank(collectTime)) {
            errorlist.add(setProblemCrFromData(dto, "5", "�ü���������޶�Ӧ�Ĳ�������", 1, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "5", "�ü���������޶�Ӧ�Ĳ�������", 1, 1));
            if (dto.getCollectTime().compareTo(dto.getCheckTime()) > 0) {
                errorlist.add(setProblemCrFromData(dto, "6", "����ʱ�䲻Ӧ���ڼ��ʱ��", 2, 0));
            }else{
                sucesslist.add(setProblemCrFromData(dto, "6", "����ʱ�䲻Ӧ���ڼ��ʱ��", 2, 1));
            }
            if (collect.isBefore(checkTime.minusHours(24))) {
                errorlist.add(setProblemCrFromData(dto, "6", "���ʱ�������ʱ������24Сʱ", 2, 0));
            }else{
                sucesslist.add(setProblemCrFromData(dto, "6", "���ʱ�������ʱ������24Сʱ", 2, 1));
            }
        }
        return new Tuple2<>(errorlist,sucesslist);
    }

    /**
     * ���ջ��ڶ���У��
     * @param dto
     * @param relationTable
     * @return
     * @throws Exception
     */
    public static Tuple2<List<ProblemDataTre>,List<ProblemDataTre>> receiveCheck(ReceiveDataId dto, Table relationTable) throws Exception {
        List<ProblemDataTre> errorlist = new ArrayList<>();
        List<ProblemDataTre> sucesslist = new ArrayList<>();
        ParamCheck check = new ParamCheck();
        //�������ݸ�ʽУ��
        //����ʽǰ�Ƚ���
        String deliPhone = AESUtil.decrypt(dto.getDeliveryPrpPhone());
        String deliId = AESUtil.decrypt(dto.getDeliveryPrpId());
        String trPhone = AESUtil.decrypt(dto.getTransportPrpPhone());
        String trId = AESUtil.decrypt(dto.getTransportPrpId());
        if (!check.checkIdCard(deliId)) {
            errorlist.add(createProblemDataTre(dto, "1", "������֤���ų�������/�������ַ������������ݱ�׼",2, 0));
        }else{
            sucesslist.add(createProblemDataTre(dto, "1", "������֤���ų�������/�������ַ������������ݱ�׼",2, 1));
        }
        if (!check.checkPhone(deliPhone)) {
            errorlist.add(createProblemDataTre(dto, "2", "�������ֻ��ź������ַ������������ݱ�׼",2, 0));
        }else{
            sucesslist.add(createProblemDataTre(dto, "2", "�������ֻ��ź������ַ������������ݱ�׼",2, 1));
        }
        if (!check.checkPhone(trPhone)) {
            errorlist.add(createProblemDataTre(dto, "4", "�������ֻ��ź������ַ������������ݱ�׼",2, 0));
        }else{
            sucesslist.add(createProblemDataTre(dto, "4", "�������ֻ��ź������ַ������������ݱ�׼",2, 0));
        }
        if (!check.checkIdCard(trId)) {
            errorlist.add(createProblemDataTre(dto, "3", "������֤���ų�������/�������ַ������������ݱ�׼",2, 0));
        }else{
            sucesslist.add(createProblemDataTre(dto, "3", "������֤���ų�������/�������ַ������������ݱ�׼",2, 1));
        }
        //���ݻ��ڶ�ӦУ��
        //��hbase��ȡ����
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
                errorlist.add(createProblemDataTre(dto, "5", "�Թ�������ת�����ݲ���Ӧ", 2, 0));
            }else{
                sucesslist.add(createProblemDataTre(dto, "5", "�Թ�������ת�����ݲ���Ӧ", 2, 1));
            }
            if (!packNum.equals(dto.getPackNum().intValue())) {
                errorlist.add(createProblemDataTre(dto, "5", "���������ת�����ݲ���Ӧ", 2, 0));
            }else{
                sucesslist.add(createProblemDataTre(dto, "5", "���������ת�����ݲ���Ӧ", 2, 1));
            }
            if (delivery.plusHours(24).isBefore(LocalDateTime.now()) && delivery.plusHours(24).isBefore(receive)) {
                errorlist.add(createProblemDataTre(dto, "6", "����ʱ��ͽ���ʱ��������24Сʱ", 2, 0));
            }else{
                sucesslist.add(createProblemDataTre(dto, "6", "����ʱ��ͽ���ʱ��������24Сʱ", 2, 1));
            }
            sucesslist.add(createProblemDataTre(dto, "8", "δ���ֶ�Ӧ��ת������", 1, 1));
        }else {
            Duration du = Duration.between(receive, LocalDateTime.now());
            long hours = du.toHours();
            if (StringUtils.isBlank(deliveryTime) && hours >= 24) {
                errorlist.add(createProblemDataTre(dto, "8", "δ���ֶ�Ӧ��ת������", 1, 0));
            }
        }
        return new Tuple2<>(errorlist,sucesslist);
    }

    /**
     * ת�˻��ڶ���У��
     * @param dto
     * @param relationTable
     * @return
     * @throws IOException
     */
    public static Tuple2<List<ProblemDataTre>,List<ProblemDataTre>> transportCheck(TransportDataId dto, Table relationTable) throws IOException {
        List<ProblemDataTre> errorlist = new ArrayList<>();
        List<ProblemDataTre> sucesslist = new ArrayList<>();
        //�������ݸ�ʽУ��
        if (dto.getDeliveryTime().compareTo(dto.getPackTime()) < 0) {
            errorlist.add(createProblemDataTre(dto, "6", "�˽������Ĵ��ʱ�����ڽ���ʱ�䣬�����ϱ�׼", 2, 0));
        }else{
            sucesslist.add(createProblemDataTre(dto, "6", "�˽������Ĵ��ʱ�����ڽ���ʱ�䣬�����ϱ�׼", 2, 1));
        }
        //���ݻ��ڶ�ӦУ��
        LocalDateTime delivery = DateUtils.getDateTimeFromDate(dto.getDeliveryTime());
        Duration du = Duration.between(delivery, LocalDateTime.now());
        long hours = du.toHours();
        //��hbase��ȡ����
        byte[] rowKey = dto.getDeliveryCode().getBytes();
        Get get = new Get(rowKey);
        Result result = relationTable.get(get);
        String receiveTime = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, "receiveTime".getBytes()));
        if (StringUtils.isBlank(receiveTime)) {
            if (hours >= 24) {
                errorlist.add(createProblemDataTre(dto, "7", "�˽���������24Сʱδ���ֽ�������", 1, 0));
            }
        }else{
            LocalDateTime receive = DateUtils.getDateTimeFromDate(receiveTime);
            if (receive.minusHours(24).isAfter(delivery)) {
                errorlist.add(createProblemDataTre(dto, "7", "�˽���������24Сʱδ���ֽ�������", 1, 0));
            }else{
                sucesslist.add(createProblemDataTre(dto, "7", "�˽���������24Сʱδ���ֽ�������", 1, 1));
            }
            for (TransportItem transportItem : dto.getTransportItem()) {
                String tubeCode = transportItem.getTubeCode();
                //��hbase��ȡ����
                byte[] tubeRowKey = tubeCode.getBytes();
                Get tubeGet = new Get(tubeRowKey);
                Result tubeResult = relationTable.get(tubeGet);
                String tubeCollectTime = Bytes.toString(tubeResult.getValue(Constants.HBASE_FAMILY, "tubeCollectTime".getBytes()));
                String collectSubmitId = Bytes.toString(tubeResult.getValue(Constants.HBASE_FAMILY, "submitId".getBytes()));
                if (StringUtils.isNotBlank(tubeCollectTime) && StringUtils.isNotBlank(collectSubmitId)) {
                    if (dto.getPackTime().compareTo(tubeCollectTime) < 0) {
                        errorlist.add(createProblemDataTre(dto, "6", "���ʱ�䲻Ӧ���ڲ�����ˮ��"+ collectSubmitId + "�Ĳ���ʱ��", 2, 0));
                    }else{
                        sucesslist.add(createProblemDataTre(dto, "6", "���ʱ�䲻Ӧ���ڲ�����ˮ��"+ collectSubmitId + "�Ĳ���ʱ��", 2, 1));
                    }
                    if (dto.getDeliveryTime().compareTo(tubeCollectTime) < 0) {
                        errorlist.add(createProblemDataTre(dto, "6", "����ʱ�䲻Ӧ���ڲ�����ˮ��"+ collectSubmitId + "�Ĳ���ʱ��", 2, 0));
                    }else{
                        sucesslist.add(createProblemDataTre(dto, "6", "����ʱ�䲻Ӧ���ڲ�����ˮ��"+ collectSubmitId + "�Ĳ���ʱ��", 2, 1));
                    }
                    LocalDateTime colt = DateUtils.getDateTimeFromDate(tubeCollectTime);
                    if (delivery.minusHours(24).isAfter(colt)) {
                        errorlist.add(createProblemDataTre(dto, "6", "ת�˵����Ӧ�Ĳ������ݵĲ��ʱ�䳬��24Сʱ", 2, 0));
                    }else{
                        sucesslist.add(createProblemDataTre(dto, "6", "ת�˵����Ӧ�Ĳ������ݵĲ��ʱ�䳬��24Сʱ", 2, 1));
                    }
                    if (hours < 24) {
                        sucesslist.add(createProblemDataTre(dto, "9", "�˽�����δ���ֶ�Ӧ�Ĳ�������", 1, 1));
                    }
                }else{
                    if (hours >= 24) {
                        errorlist.add(createProblemDataTre(dto, "9", "�˽�����δ���ֶ�Ӧ�Ĳ�������", 1, 0));
                    }
                }
            }
        }
        return new Tuple2<>(errorlist,sucesslist);
    }

    /**
     * �ɼ����ڶ���У��
     * @param dto
     * @param relationTable
     * @return
     * @throws Exception
     */
    public static Tuple2<List<ProblemDataCr>,List<ProblemDataCr>> collectCheck(CollectDataId dto, Table relationTable) throws Exception {
        ParamCheck check = new ParamCheck();
        List<ProblemDataCr> errorlist = new ArrayList<>();
        List<ProblemDataCr> sucesslist = new ArrayList<>();
        //�������ݸ�ʽУ��
        //����ʽǰ�Ƚ���
        String name = AESUtil.decrypt(dto.getPersonName());
        String phone = AESUtil.decrypt(dto.getPersonPhone());
        String idCard = AESUtil.decrypt(dto.getPersonIdCard());
        if (StringUtils.isNotEmpty(idCard) && check.checkNotHaveString(idCard)) {
            errorlist.add(setProblemCrFromData(dto, "1", "��������֤�����в������ַ�", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "1", "��������֤�����в������ַ�", 2, 1));
        }
        if (StringUtils.isNotEmpty(phone) && check.checkNotHaveString(phone)) {
            errorlist.add(setProblemCrFromData(dto, "3", "���������ֻ����в������ַ�", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "3", "���������ֻ����в������ַ�", 2, 1));
        }
        if ((Objects.isNull(dto.getCollectCount()) && dto.getCollectCount() == 0)
                || Objects.isNull(dto.getCollectLimitnum()) && dto.getCollectLimitnum() == 0) {
            errorlist.add(setProblemCrFromData(dto, "7", "���������Թ�������Ϊ�ջ�Ϊ0", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "7", "���������Թ�������Ϊ�ջ�Ϊ0", 2, 1));
        }
        if (StringUtils.isNotEmpty(name) && check.checkNotHaveString(name)) {
            errorlist.add(setProblemCrFromData(dto, "2", "�������������в������ַ�", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "2", "�������������в������ַ�", 2, 1));
        }
        if (dto.getAddTime().compareTo(dto.getCollectTime()) < 0) {
            errorlist.add(setProblemCrFromData(dto, "6", "���������ϱ�ʱ�䲻Ӧ���ڲ���ʱ��", 2, 0));
        }else{
            sucesslist.add(setProblemCrFromData(dto, "6", "���������ϱ�ʱ�䲻Ӧ���ڲ���ʱ��", 2, 1));
        }
        //���ݻ��ڶ�ӦУ��
        LocalDateTime collect = DateUtils.getDateTimeFromDate(dto.getCollectTime());
        Duration du = Duration.between(collect, LocalDateTime.now());
        long hours = du.toHours();
        //��hbase��ȡ����
        StringJoiner sj = new StringJoiner(Constants.HBASE_KEY_SPLIT);
        sj.add(dto.getPersonIdCard());
        sj.add(dto.getTubeCode());
        byte[] rowKey = sj.toString().getBytes();
        Get get = new Get(rowKey);
        Result result = relationTable.get(get);
        String reportTime = Bytes.toString(result.getValue(Constants.HBASE_FAMILY, "checkTime".getBytes()));
        if (hours >= 24) {
            if (StringUtils.isBlank(reportTime)) {
                errorlist.add(setProblemCrFromData(dto, "4", "�ò������ݳ�24Сʱ�޶�Ӧ�ļ��������", 1, 0));
            } else {
                LocalDateTime checkTime = DateUtils.getDateTimeFromDate(reportTime);
                if (checkTime.minusHours(24).isAfter(collect)) {
                    errorlist.add(setProblemCrFromData(dto, "4", "�ò������ݳ�24Сʱ�޶�Ӧ�ļ��������",1, 0));
                }else{
                    sucesslist.add(setProblemCrFromData(dto, "4", "�ò������ݳ�24Сʱ�޶�Ӧ�ļ��������",1, 1));
                }
            }
        }
        return new Tuple2<>(errorlist, sucesslist);
    }

    /**
     * ���òɼ�����
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
        data.setAdd_time(dto.getAddTime());
        data.setProblem_record(record);
        data.setIs_valid(isValid);
        data.setIs_delete(0);
        data.setCreate_by(dto.getUserName());
        data.setUpdate_by(dto.getUserName());
        data.setUpdate_time(Constants.FASTDATEFORMAT.format(new Date()));
        return data;
    }

    /**
     * ���ü�⹤��
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
        data.setAdd_time(dto.getAddTime());
        data.setProblem_record(record);
        data.setIs_valid(isValid);
        data.setIs_delete(0);
        data.setCreate_by(dto.getUserName());
        data.setUpdate_by(dto.getUserName());
        data.setUpdate_time(Constants.FASTDATEFORMAT.format(new Date()));
        return data;
    }

    /**
     * ����ת�˹���
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
        data.setAdd_time(dto.getAddTime());
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
     * ���ý��չ���
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
        data.setAdd_time(dto.getAddTime());
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
