package com.sxhs.realtime.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Description: 参数验证类
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/29 10:53
 */
public class ParamCheck {

    public static Boolean checkIpAddress(String address) {
        String regex = "((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(address);
        return m.matches();
    }

    /*验证手机号*/
    public Boolean checkPhone(String personPhone){
        String regex = "^((13[0-9])|(14[0-1,4-9])|(15[0-3,5-9])|(16[2,5-7])|(17[0-8])|(18[0-9])|(19[0-3,5-9]))\\d{8}$";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(personPhone);
        return m.matches();
    }

    /*验证证件号*/
    public Boolean checkIdCard(String personIdCard){
        /*18位身份证验证*/
        String idEight="^[1-9][0-9]{5}(18|19|20)[0-9]{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)[0-9]{3}([0-9]|(X|x))$";
        String idFifteen="^[1-9]\\d{7}((0\\d)|(1[0-2]))(([0-2]\\d)|3[0-1])\\d{3}$";
        Pattern pattern = Pattern.compile(idEight);
        Pattern pat = Pattern.compile(idFifteen);
        if (!pattern.matcher(personIdCard).matches()) {
            return pat.matcher(personIdCard).matches();
        }
        return pattern.matcher(personIdCard).matches();
    }

//    /*上报时间*/
//    public Boolean checkAddTime(Date addTime){
//        String rexp = "^((([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})-(((0[13578]|1[02])-(0[1-9]|[12][0-9]|3[01]))|((0[469]|11)-(0[1-9]|[12][0-9]|30))|(02-(0[1-9]|[1][0-9]|2[0-8]))))|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))-02-29))\\s+([0-1]?[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])$";
//        return Pattern.matches(rexp, DateUtil.getDateFormat(addTime, DateUtil.FULL_TIME_SPLIT_PATTERN));
//    }

    /*行政区域代码*/
    public Boolean checkAreaId(Long areaId){
        long start = 6101L, end = 6112L;
        return areaId >= start && areaId <= end;
    }

    /*检测纯数字*/
    public Boolean checkIsNumber(String code){
        if (StringUtils.isEmpty(code)) {
            return false;
        }
        String rexp = "[0-9]*";
        String regexp = "^(\\d+,)*\\d+$";
        if(!Pattern.matches(rexp, code)) {
            return Pattern.matches(regexp, code);
        }
        return true;
    }

    /*检测特殊符号*/
    public Boolean checkNotHaveString(String code){
        if (StringUtils.isEmpty(code)) {
            return false;
        }
        String regexp = ".*[`~!@#$%^&*()+=|{}':;,\\[\\].<>/?￥…—【】‘；：”“’。，、？\\\\]+.*";
        return Pattern.matches(regexp, code);
    }
}
