package com.xy.lr.java.spider.entity;

import com.xy.lr.java.hbase.OrganizationParser;
import com.xy.lr.java.tools.file.JFile;
import org.dom4j.Element;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by hadoop on 11/13/16.
 */
public class SpiderBasicInformation {
    private List<SpiderAuthor> spiderAuthors;

    private List<String> chineseName;

    private List<String> englishName;

    private String spiderOrganization;

    private String spiderPublishingHouse;

    private List<String> spiderIndexs;

    private String spiderTime;

    private String spiderSortNumber;

    private List<String> spiderKeyWords;

    private String spiderFundsProject;

    private String spiderAbstracts;

    public SpiderBasicInformation() {
        this.spiderAuthors = new ArrayList<SpiderAuthor>();
        this.chineseName = new ArrayList<String>();
        this.englishName = new ArrayList<String>();
        this.spiderOrganization = "null";
        this.spiderPublishingHouse = "null";
        this.spiderIndexs = new ArrayList<String>();
        this.spiderTime = "null";
        this.spiderSortNumber = "null";
        this.spiderKeyWords = new ArrayList<String>();
        this.spiderFundsProject = "null";
        this.spiderAbstracts = "null";
    }

    /**
     * 设置基本信息
     * @param basicElement 基本信息
     */
    public boolean setBasicInformation(Element basicElement) {
        Iterator iterator = basicElement.elementIterator();

        while(iterator.hasNext()) {
            Element element = (Element) iterator.next();

//            System.out.println(element.getName());
            if(element.getName().equals("Authors")) {//中文名
                Iterator it = element.elementIterator();
                while(it.hasNext()) {
                    Element tmpElement = (Element) it.next();
                    this.chineseName.add(tmpElement.getStringValue());
                }
            } else if(element.getName().equals("AuthorsEng")) {//英文名
                Iterator it = element.elementIterator();
                while(it.hasNext()) {
                    Element tmpElement = (Element) it.next();
                    this.englishName.add(tmpElement.getStringValue());
                }
            } else if(element.getName().equals("Organization")) {//组织
                Iterator it = element.elementIterator();
                while(it.hasNext()) {
                    Element tmpElement = (Element) it.next();
                    this.spiderOrganization = tmpElement.getStringValue();
                }
            } else if(element.getName().equals("PublishingHouse")) {//PublishingHouse
                Iterator it = element.elementIterator();
                while(it.hasNext()) {
                    Element tmpElement = (Element) it.next();
                    this.spiderPublishingHouse = tmpElement.getStringValue();
                }
            } else if(element.getName().equals("Index")) {//Index
                Iterator it = element.elementIterator();
                while(it.hasNext()) {
                    Element tmpElement = (Element) it.next();
                    this.spiderIndexs.add(tmpElement.getStringValue());
                }
            } else if(element.getName().equals("Time")) {//Time
                Iterator it = element.elementIterator();
                while(it.hasNext()) {
                    Element tmpElement = (Element) it.next();
                    this.spiderTime = tmpElement.getStringValue();
                }
            } else if(element.getName().equals("Sortnumber")) {//SortNumber
                Iterator it = element.elementIterator();
                while(it.hasNext()) {
                    Element tmpElement = (Element) it.next();
                    this.spiderSortNumber = tmpElement.getStringValue();
                }
            } else if(element.getName().equals("Keyword")) {//关键词
                Iterator it = element.elementIterator();
                while(it.hasNext()) {
                    Element tmpElement = (Element) it.next();
                    this.spiderKeyWords.add(tmpElement.getStringValue());
                }
            } else if(element.getName().equals("Fundsproject")) {//FundsProject
                Iterator it = element.elementIterator();
                while(it.hasNext()) {
                    Element tmpElement = (Element) it.next();
                    this.spiderFundsProject = tmpElement.getStringValue();
                }
            } else if(element.getName().equals("Abstracts")) {//摘要
                Iterator it = element.elementIterator();
                while(it.hasNext()) {
                    Element tmpElement = (Element) it.next();
                    this.spiderAbstracts = tmpElement.getStringValue();
                }
            }
        }

        return setAuthors(this.chineseName, this.englishName, this.spiderOrganization);
    }

    /**
     * 设置作者信息
     * @param cName 中文名
     * @param eName 英文名
     * @param organization 组织机构
     */
    public boolean setAuthors(List<String> cName, List<String> eName,
                           String organization) {
        JFile.appendFile("xmlParsed.xml", organization);
        if(cName.size() != eName.size()) {
//            System.err.println("authors error!");
            return false;
        }else {
            if(organization.indexOf(")") == -1) {
                for(int i = 0; i < eName.size() ;i++) {
                    SpiderAuthor spiderAuthor = new SpiderAuthor();

                    spiderAuthor.setcName(cName.get(i));
                    spiderAuthor.seteName(eName.get(i));
                    spiderAuthor.setOrgization(organization);

                    this.spiderAuthors.add(spiderAuthor);
                }
            }else if(organization.length() == 0) {
                return false;
            }else {
                Map<String, String> map = new OrganizationParser().parserOrganization(organization);
                JFile.appendFile("xmlParsed.xml", map.toString());

                for(int i = 0; i < cName.size() ;i++) {
                    SpiderAuthor spiderAuthor = new SpiderAuthor();

                    spiderAuthor.setcName(cName.get(i));
                    spiderAuthor.seteName(eName.get(i));

//                    System.out.println(map.get(cName.get(i).trim()));
                    if(map.containsKey(cName.get(i).trim())){
                        spiderAuthor.setOrgization(map.get(cName.get(i).trim()));
                    }else {
                        spiderAuthor.setOrgization("null");
                    }

                    this.spiderAuthors.add(spiderAuthor);
                }
            }

            return true;
        }
    }

    public void printAll() {
        System.out.println("---------------------------------------");
        System.out.println("|" + this.spiderPublishingHouse +"|");
        System.out.println("|" + this.spiderIndexs +"|");
        System.out.println("|" + this.spiderTime +"|");
        System.out.println("|" + this.spiderSortNumber +"|");
        System.out.println("|" + this.spiderFundsProject +"|");
        System.out.println("|" + this.spiderAbstracts +"|");
        System.out.println("---------------------------------------");
        for(String string : this.spiderKeyWords){
            System.out.println(string);
        }
        System.out.println("---------------------------------------");
        for (String string : this.chineseName) {
            System.out.println(string);
        }
    }

    public List<String> getChineseName() {
        return chineseName;
    }

    public void setChineseName(List<String> chineseName) {
        this.chineseName = chineseName;
    }

    public List<String> getEnglishName() {
        return englishName;
    }

    public void setEnglishName(List<String> englishName) {
        this.englishName = englishName;
    }

    public String getSpiderPublishingHouse() {
        return spiderPublishingHouse;
    }

    public void setSpiderPublishingHouse(String spiderPublishingHouse) {
        this.spiderPublishingHouse = spiderPublishingHouse;
    }

    public List<String> getSpiderIndex() {
        return spiderIndexs;
    }


    public String getSpiderTime() {
        return spiderTime;
    }

    public void setSpiderTime(String spiderTime) {
        this.spiderTime = spiderTime;
    }

    public String getSpiderSortNumber() {
        return spiderSortNumber;
    }

    public void setSpiderSortNumber(String spiderSortNumber) {
        this.spiderSortNumber = spiderSortNumber;
    }

    public List<String> getSpiderKeyWords() {
        return spiderKeyWords;
    }

    public void setSpiderKeyWords(List<String> spiderKeyWords) {
        this.spiderKeyWords = spiderKeyWords;
    }

    public String getSpiderFundsProject() {
        return spiderFundsProject;
    }

    public void setSpiderFundsProject(String spiderFundsProject) {
        this.spiderFundsProject = spiderFundsProject;
    }

    public String getSpiderAbstracts() {
        return spiderAbstracts;
    }

    public void setSpiderAbstracts(String spiderAbstracts) {
        this.spiderAbstracts = spiderAbstracts;
    }

    public List<SpiderAuthor> getSpiderAuthors() {
        return this.spiderAuthors;
    }
}
