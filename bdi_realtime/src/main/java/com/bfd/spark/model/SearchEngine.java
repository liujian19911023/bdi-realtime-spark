package com.bfd.spark.model;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchEngine {
	private static Pattern p = Pattern.compile("(?<=//|)((\\w)+\\.)+\\w+");
    public static String getUrlHost(String url){
        if (url == null){
            return null;
        }

        Matcher m = p.matcher(url);
        if (m.find()){
            return m.group();
        }

        return null;
    }

    public static String getSearchEngineName(String url,HashMap<String,String> searchEngine){
        String host = getUrlHost(url);
        if (host == null){
            return null;
        }
        return searchEngine.get(host);

    }
    public static int getLinkType(String entryPage, String linkPage,HashMap<String,String> searchEngine){
        if (linkPage == null){
            return Constant.BDI_MEDIA_LINK_TYPE_DIRECT;
        }
        String entryPageHost = SearchEngine.getDomain(entryPage);
        String linkPageHost = SearchEngine.getDomain(linkPage);
        if (linkPageHost == null){
            return Constant.BDI_MEDIA_LINK_TYPE_DIRECT;
        }

        if (linkPageHost.equals(entryPageHost)){
            return Constant.BDI_MEDIA_LINK_TYPE_DIRECT;
        }

        return getSearchEngineName(linkPage,searchEngine) == null ?
                Constant.BDI_MEDIA_LINK_TYPE_EXTERNAL :
                Constant.BDI_MEDIA_LINK_TYPE_SEARCH;
    }
     public static String getDomain(String url){
        String host = getUrlHost(url);
        if (host == null){
            return null;
        }
        String [] strArr = host.split("\\.");
        if (strArr.length < 2){
            return null;
        }
        return strArr[1];
    }
}
