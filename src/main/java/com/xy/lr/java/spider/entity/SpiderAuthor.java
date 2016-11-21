package com.xy.lr.java.spider.entity;

/**
 * Created by hadoop on 11/14/16.
 */
public class SpiderAuthor {
    private String cName;
    private String eName;
    private String orgization;

    public SpiderAuthor() {
        this.cName = "null";
        this.eName = "null";
        this.orgization = "null";
    }

    public String getcName() {
        return cName;
    }

    public void setcName(String cName) {
        this.cName = cName;
    }

    public String geteName() {
        return eName;
    }

    public void seteName(String eName) {
        this.eName = eName;
    }

    public String getOrgization() {
        return orgization;
    }

    public void setOrgization(String orgization) {
        this.orgization = orgization;
    }
}
