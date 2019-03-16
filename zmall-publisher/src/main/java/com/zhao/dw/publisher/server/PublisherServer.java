package com.zhao.dw.publisher.server;

import java.util.Map;

public interface PublisherServer {
    public int getDauTotal(String date);
    public Map getDauHours(String date);
}
