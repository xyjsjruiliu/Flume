package com.xy.lr.java.spider;

import com.xy.lr.java.spider.entity.SpiderAuthor;
import com.xy.lr.java.spider.entity.SpiderBasicInformation;
import org.dom4j.Element;

import java.util.Iterator;
import java.util.List;

/**
 * SpiderConfiguration
 * Created by hadoop on 11/13/16.
 */
public class SpiderConfiguration {
    private String title;

    private SpiderBasicInformation spiderBasicInformation;

    public SpiderConfiguration() {
        this.title = "null";
        this.spiderBasicInformation = new SpiderBasicInformation();
    }

    /**
     *
     * @param confElement
     */
    public boolean setConfiguration(Element confElement) {
        Iterator iterator = confElement.elementIterator();

        while(iterator.hasNext()) {
            Element element = (Element) iterator.next();

//            System.out.println(element.getName());
            if(element.getName().equals("title")) {
                this.title = element.getStringValue();
            } else if(element.getName().equals("BasicInformation")) {
                //处理失败
                if(!this.spiderBasicInformation.setBasicInformation(element)){
                    return false;
                }
            }
        }

        //处理正确
        return true;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * 获取摘要
     * @return 摘要
     */
    public String getAbstracts() {
        return this.spiderBasicInformation.getSpiderAbstracts();
    }

    /**
     * 获取Index
     * @return index
     */
    public String getIndex() {
        StringBuilder stringBuilder = new StringBuilder();
        for(String temp : this.spiderBasicInformation.getSpiderIndex()) {
            stringBuilder.append(temp + ",");
        }
        if(stringBuilder.length() == 0) {
            return "null";
        }else {
            return stringBuilder.substring(0, stringBuilder.length() - 1);
        }

    }

    /**
     * 获取Time
     * @return time
     */
    public String getTime() {
        return this.spiderBasicInformation.getSpiderTime();
    }

    /**
     * 获取SortNumber
     * @return sortnumber
     */
    public String getSortNumber() {
        return this.spiderBasicInformation.getSpiderSortNumber();
    }

    /**
     * 获取关键词
     * @return 关键词
     */
    public String getKeyWords() {
        StringBuilder stringBuilder = new StringBuilder();
        for(String temp : this.spiderBasicInformation.getSpiderKeyWords()) {
            stringBuilder.append(temp + ",");
        }
//        System.out.println(stringBuilder);
        if(stringBuilder.length() == 0){
            return "null";
        }else {
            return stringBuilder.substring(0, stringBuilder.length() - 1);
        }
    }

    /**
     * 获取作者信息
     * @return 作者信息列表
     */
    public List<SpiderAuthor> getSpiderAuthors() {
        return this.spiderBasicInformation.getSpiderAuthors();
    }

    /**
     * 获取
     * @return
     */
    public String getPublishingHouse() {
        return this.spiderBasicInformation.getSpiderPublishingHouse();
    }

    /**
     * 获取FundsProject
     * @return fundsproject
     */
    public String getFundsProject() {
        return this.spiderBasicInformation.getSpiderFundsProject();
    }

}
