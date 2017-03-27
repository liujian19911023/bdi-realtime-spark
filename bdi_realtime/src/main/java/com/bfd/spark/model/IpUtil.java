package com.bfd.spark.model;

import java.util.List;

import org.apache.commons.lang.StringUtils;

public class IpUtil {
	private  long convertIpToInt(String ipAddressStr) {
		if (StringUtils.isEmpty(ipAddressStr)) {
			return -1;
		}

		// 拆分ip地址
		String[] ipItemArray = ipAddressStr.split("\\.");
		if (ipItemArray.length != 4) {
			return -1;
		}

		return Long.parseLong(ipItemArray[0]) << 24
				| Long.valueOf(ipItemArray[1]) << 16
				| Long.valueOf(ipItemArray[2]) << 8
				| Long.valueOf(ipItemArray[3]);
	}

	public  LocationInfo convertIpToLoc(String ipAddressStr,
			List<IPRecord> IP_INT_ARRAY) {
		long ipInt = convertIpToInt(ipAddressStr);
		if (ipInt == -1) {
			return null;
		}

		return locationBinarySearch(ipInt, IP_INT_ARRAY);
	}

	private  LocationInfo locationBinarySearch(long ipInt,
			List<IPRecord> IP_INT_ARRAY) {
		int low = 0;
		int high = IP_INT_ARRAY.size() - 1;
		// 去掉不在区间内的情况
		if (ipInt < IP_INT_ARRAY.get(low).ipBegin
				|| ipInt > IP_INT_ARRAY.get(high).ipEnd) {
			return null;
		}

		int middle = -1;
		while (low <= high) {
			middle = (low + high) / 2;

			IPRecord ipPair = IP_INT_ARRAY.get(middle);
			if (ipPair.isIPMatch(ipInt)) {
				return new LocationInfo(ipPair.loc_pro, ipPair.isForeign);
			}
			if (ipInt < ipPair.ipBegin) {
				high = middle - 1;
			} else {
				low = middle + 1;
			}
		}

		if (low > IP_INT_ARRAY.size() || high < 0) {
			return null;
		}

		IPRecord ipPair = IP_INT_ARRAY.get(middle);
		if (ipPair.isIPMatch(ipInt)) {
			return new LocationInfo(ipPair.loc_pro, ipPair.isForeign);
		} else {
			return null;
		}
	}

}
