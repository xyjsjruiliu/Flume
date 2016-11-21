package com.xy.lr.java.spider;

import com.xy.lr.java.tools.file.JFile;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;

/**
 * Created by hadoop on 11/13/16.
 */
public class SpiderXMLParser {
    //创建SAXReader的对象saxReader
    private SAXReader saxReader;

    //xml 文件对象
    private Document document;

    public SpiderXMLParser() {
        saxReader = new SAXReader();
    }

    public SpiderConfiguration parser(String filePath) {
        //通过saxReader对象的
        try {
            document = saxReader.read( new File( filePath ) );
        } catch (DocumentException e) {
            if (new File(filePath).exists()){
                new File(filePath).delete();
            }
            JFile.appendFile("errorXML", filePath);
        }
        //通过 document 对象获取根节点 rootElement
        Element xmlDocumentRoot = document.getRootElement();

        //XMLDocument 对象
        SpiderConfiguration conf = new SpiderConfiguration();
        //设置内容
        if(!conf.setConfiguration(xmlDocumentRoot)){
            conf = null;
        }

        return conf;
    }

    public static void main(String[] args) {
        SpiderXMLParser spiderXMLParser = new SpiderXMLParser();

        spiderXMLParser.parser("document.xml");
    }
}
