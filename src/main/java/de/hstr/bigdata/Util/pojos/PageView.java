package de.hstr.bigdata.Util.pojos;

import myapps.PageViewTypedDemo;

public class PageView implements PageViewTypedDemo.JSONSerdeCompatible {
    public String user;
    public String page;
    public Long timestamp;

    @Override
    public String toString() {
        return "PageView{" +
                "user='" + user + '\'' +
                ", page='" + page + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
