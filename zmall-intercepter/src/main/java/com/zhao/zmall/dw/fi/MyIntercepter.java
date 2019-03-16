package com.zhao.zmall.dw.fi;


import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyIntercepter implements Interceptor {

    public static final String FI_HEADER_TYPE = "logType";
    public static final String FI_HEADER_STARTUP = "startup";
    public static final String FI_HEADER_EVENT = "event";
    Gson gson = null;
    @Override
    public void initialize() {
        gson = new Gson();
    }

    @Override
    public Event intercept(Event event) {
        String logString = new String(event.getBody());
        HashMap logMap = gson.fromJson(logString, HashMap.class);
        String  type = (String) logMap.get("type");
        Map<String, String> headers = event.getHeaders();
        if(type.equals(FI_HEADER_STARTUP)){
            headers.put(FI_HEADER_TYPE,FI_HEADER_STARTUP);
        }else{
            headers.put(FI_HEADER_TYPE, FI_HEADER_EVENT);
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    /**
     * 通过该静态内部类来创建自定义对象供flume使用，实现Intercepter.Builder接口，并实现其抽象方法
     */

    public static class Builder implements Interceptor.Builder{

        /**
         * 该方法主要用来返回创建的自定义类拦截器对象
         * @return
         */
        @Override
        public Interceptor build() {

            return new MyIntercepter();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
