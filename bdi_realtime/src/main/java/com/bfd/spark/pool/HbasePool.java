package com.bfd.spark.pool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

public class HbasePool {

	private static HTable table = null;

	private HbasePool() {
	}

	public synchronized static HTable getHbaseClient(
			String hbase_zookeeper_quorum, String zookeeper_znode_parent,
			String hbase_rootdir, String hbase_zookeeper_property_clientPort,
			String table_name) {
		if (table == null) {
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", hbase_zookeeper_quorum);
			conf.set("zookeeper.znode.parent", zookeeper_znode_parent);
			conf.set("hbase.rootdir", hbase_rootdir);
			conf.set("hbase.zookeeper.property.clientPort",
					hbase_zookeeper_property_clientPort);
			try {
				table = new HTable(conf, table_name);
				Thread thread = new Thread() {
					public void run() {
						try {
							table.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
						table = null;
					}
				};
				Runtime.getRuntime().addShutdownHook(thread);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return table;
	}

}
