package com.bfd.spark.model;

import java.io.Serializable;

public class IPRecord implements Serializable {
	private final static String IP_SEQARATOR = " ";
	public static String China = "中国";

	boolean updateIPRecord(String lineText) {
		// 处理这一行的信息
		String[] lineSplitArray = lineText.split(IP_SEQARATOR);
		if (lineSplitArray.length <= 4) {
			return false;
		}

		ipBegin = Long.parseLong(lineSplitArray[0]);
		ipEnd = Long.parseLong(lineSplitArray[1]);
		String country = lineSplitArray[2];
		country = country.equals("*") ? "其他" : country;
		String proc = lineSplitArray[4];
		proc = proc.equals("*") ? "其他" : proc;

		if (country.equals((China))) {
			loc_pro = proc;
		} else {
			loc_pro = country;
			isForeign = true;
		}

		return true;
	}

	boolean isIPMatch(long ip) {
		return ip >= ipBegin && ip <= ipEnd;
	}

	 long ipBegin;
	 long ipEnd;

	 String loc_pro;
	 boolean isForeign = false;
	 
	 public String toString(){
		 return ipBegin+" "+ipEnd+" "+loc_pro+" "+isForeign;
	 }

}
