package com.sxhs.realtime.util;

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
        return LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
