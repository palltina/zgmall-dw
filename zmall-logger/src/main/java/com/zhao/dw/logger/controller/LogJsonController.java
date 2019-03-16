package com.zhao.dw.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.constant.ZmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LogJsonController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LogJsonController.class);

    @PostMapping("/log")
    public void shipLog(@RequestParam("log") String log){
        JSONObject logJsonObj = JSON.parseObject(log);
        logJsonObj.put("ts",System.currentTimeMillis());
        String logNew = logJsonObj.toJSONString();

        //将日志发送到kafka中
        sendKafka(logJsonObj);
        logger.info(logNew);
    }


    public void sendKafka(JSONObject jsonObject) {

        //如果日志类型是startup类型，就将其发送到kafka的startup主题中
        if(jsonObject.getString("type").equals("startup")){
            kafkaTemplate.send(ZmallConstant.KAFKA_TOPIC_STARTUP, jsonObject.toJSONString());
        }else{
        //如果日志类型是event，就将其发送到event主题中
            kafkaTemplate.send(ZmallConstant.KAFKA_TOPIC_EVENT, jsonObject.toJSONString());
        }
    }
}
