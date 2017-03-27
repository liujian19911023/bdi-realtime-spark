package com.bfd.spark.model;

import java.io.Serializable;

public class LocationInfo implements Serializable{
    public boolean isForeign = false;
    public String locPro = null;

    public LocationInfo(String province, boolean isForeign){
        if (province != null && !province.equals("None")){
            this.locPro = province;
        }

        this.isForeign = isForeign;
    }


    @Override
    public String toString() {
        return "LocationInfo{" +
                "isForeign=" + isForeign +
                ", locPro='" + locPro + '\'' +
                '}';
    }
}