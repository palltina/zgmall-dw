package com.zhao.dw.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhao.dw.publisher.server.PublisherServer;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RestController
public class PublisherController {

    @Autowired
    PublisherServer publisherServer;

    @GetMapping("realtime-hours")
    public String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date") String date){
        Map dauHoursMap = new HashMap();
        if("dau".equals(id)){


            //获得每小时的日活
            Map dauHoursTodayMap = publisherServer.getDauHours(date);
            Date today = null;
            try {
               today = new SimpleDateFormat("yyyy-MM-dd").parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Date yesterday = DateUtils.addDays(today,-1);
            String yesterdayDate = new SimpleDateFormat("yyyy-MM-dd").format(yesterday);
            Map dauHoursYesterdayMap = publisherServer.getDauHours(yesterdayDate);
            dauHoursMap.put("yesterday", dauHoursYesterdayMap);
            dauHoursMap.put("today", dauHoursTodayMap);
        }
        return JSON.toJSONString(dauHoursMap);
    }

}
