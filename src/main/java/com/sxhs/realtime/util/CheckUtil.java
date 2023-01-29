package com.sxhs.realtime.util;

import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Objects;

public class CheckUtil {
    /**
     * 采样与检测结果数据中身份证号需符合国家身份证号格式
     * 采样与检测结果数据中手机号码需符合手机号码规定格式
     * 采样与检测结果数据中被检人姓名中不能出现特殊字符
     *
     * @param personIdCard
     * @param personPhone
     * @param personName
     * @return
     */
    // TODO 测试未通过：样例数据解密后都为null
    public static Boolean checkIdPhoneName(String personIdCard, String personPhone, String personName) {
        try {
            String name = AESUtil.decrypt(personName);
            String phone = AESUtil.decrypt(personPhone);
            String idCard = AESUtil.decrypt(personIdCard);

            ParamCheck check = new ParamCheck();
            if (StringUtils.isNotEmpty(idCard) && check.checkNotHaveString(idCard)) {
                return false;
            }
            if (StringUtils.isNotEmpty(phone) && check.checkNotHaveString(phone)) {
                return false;
            }
            if (StringUtils.isNotEmpty(name) && check.checkNotHaveString(name)) {
                return false;
            }
        } catch (Exception ignored) {
        }
        return true;
    }

    /**
     * 采样数据中采样时间不可晚于上报时间
     *
     * @param collectTime yyyy-MM-dd HH:mm:ss
     * @param addTime     yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static boolean checkCollectTime(String collectTime, String addTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            if (sdf.parse(collectTime).getTime() > sdf.parse(addTime).getTime()) {
                return false;
            }
        } catch (ParseException ignored) {
        }
        return true;
    }

    /**
     * 采样与检测结果数据中试管样本数不能为0或为空(包括实际与计划样本数)
     * 原判断逻辑可能有点问题，代码如下：
     * if ((Objects.isNull(dto.getCollectCount()) && dto.getCollectCount() == 0)
     * || Objects.isNull(dto.getCollectLimitnum()) && dto.getCollectLimitnum() == 0) {
     * list.add(setProblemCrFromData(dto, "7", "此数据里试管样本数为空或为0", 2));
     * }
     *
     * @param collectCount
     * @param collectLimitnum
     * @return
     */
    public static boolean checkCollectCount(Integer collectCount, Integer collectLimitnum) {
        if (Objects.isNull(collectCount) || Objects.isNull(collectLimitnum) || collectCount == 0 || collectLimitnum == 0) {
            return false;
        }
        return true;
    }

    /**
     * 检测结果中检测时间不可晚于上报时间
     * 检测结果中打包时间不可早于采样时间
     * 检测结果中接收时间不可早于采样时间
     * 检测结果中检测时间不可早于采样时间
     * 检测结果中检测时间不可早于打包时间
     * 检测结果中检测时间不可早于接收时间
     * 检测结果中检测时间与采样时间间隔不低于60分钟
     *
     * @param checkTime   检测时间
     * @param addTime     上报时间
     * @param packTime    打包时间
     * @param collectTime 采样时间
     * @param receiveTime 接收时间
     * @return
     */
    public static boolean checkReportTime(String checkTime, String addTime, String packTime, String collectTime, String receiveTime) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            long check = sdf.parse(checkTime).getTime();
            long add = sdf.parse(addTime).getTime();
            long pack = sdf.parse(packTime).getTime();
            long collect = sdf.parse(collectTime).getTime();
            long receive = sdf.parse(receiveTime).getTime();
            if (check > add || pack > collect || receive > collect || check > collect || check > pack || check > receive || ((check - collect) < 60 * 60 * 1000)) {
                return false;
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return true;
    }
}
