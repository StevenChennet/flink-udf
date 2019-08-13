package com.teld.bdp.metric;

public class ThirdServiceMock {
    public String callService(String name) {
        String desc = "";
        switch (name) {
            case "zhansan":
                desc = "张三";
                break;
            case "lisi":
                desc = "李四";
                break;
            case "wangwu":
                desc = "王五";
                break;
            case "maliu":
                desc = "马六";
                break;
            case "zhouqi":
                desc = "周七";
                break;
            default:
                desc = "unknown";
                break;
        }
        return desc;
    }
}
