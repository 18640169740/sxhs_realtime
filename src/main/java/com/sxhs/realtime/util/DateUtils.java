package com.sxhs.realtime.util;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @Description:
 * @Author: zhangJunWei
 * @CreateTime: 2023/1/29 17:06
 */
public class DateUtils {

    /**
     * 时间字符串转LocalDateTime
     * @param dateTimeStr
     * @return
     */
    public static LocalDateTime getDateTimeFromDate(String dateTimeStr){
        if(StringUtils.isBlank(dateTimeStr)){
            return null;
        }
        return LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
