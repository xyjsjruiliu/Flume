package com.xy.lr.java.hbase;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hadoop on 11/14/16.
 */
public class OrganizationParser {
    public static void main(String[] args) {
        Map<String, String> map = new OrganizationParser().parserOrganization("" +
                "尹吉林,王欣璐,YIN Ji-lin,WANG Xin-lu(广州军区广州总医院PET/CT中心,广东广州,510010) 王成,WANG Cheng(上海交通大学医学院附属仁济医院核医学科,上海,200127) ");

        for(Map.Entry<String, String> entry : map.entrySet()) {
            System.out.println(entry.getKey() + "\t" + entry.getValue());
        }
    }

    public Map<String, String> parserOrganization(String resources) {


        String[] resourceSplit = resources.split("\\)");
//        System.out.println(resourceSplit.length);

        Map<String, String> nameToOrg = new HashMap<String, String>();

        for(String source : resourceSplit) {
//            System.out.println(s);
            String[] sourceSplit = source.split("\\(");

            if(sourceSplit.length != 2){
                continue;
            }

            String nameResource = sourceSplit[0];
            String org = sourceSplit[1];

            if(nameResource.split(",").length > 2) {
                if(nameResource.split(",").length % 2 == 0) {
                    String[] finalSplit = nameResource.split(",");

                    for(int i = 0 ;i < finalSplit.length / 2 ; i++) {
                        String cName = finalSplit[i];
                        nameToOrg.put(cName.trim(), org);
                    }
                }
            }else if(nameResource.split(",").length == 2){
//                System.out.println(sourceSplit);
//                System.out.println(nameResource);
                String cName = nameResource.split(",")[0];

                nameToOrg.put(cName.trim(), org);
            }

//            System.out.println("--------------------------------------------");
        }
        return nameToOrg;
    }
}
