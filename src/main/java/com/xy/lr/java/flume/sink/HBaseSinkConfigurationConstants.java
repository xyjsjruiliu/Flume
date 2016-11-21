package com.xy.lr.java.flume.sink;

import org.apache.hadoop.hbase.HConstants;

/**
 * Created by hadoop on 11/14/16.
 */
public class HBaseSinkConfigurationConstants {

    public static final String HBASE_CONF_PATH = "hbaseconf";

    public static final String HADOOP_CONF_PATH = "hadoopconf";
    /**
     * The Hbase table which the sink should write to.
     */
    public static final String CONFIG_TABLE = "table";
    /**
     * The column family which the sink should use.
     */
    public static final String CONFIG_COLUMN_FAMILY = "columnFamily";
    /**
     * Maximum number of events the sink should take from the channel per
     * transaction, if available.
     */
    public static final String CONFIG_BATCHSIZE = "batchSize";
    /**
     * The fully qualified class name of the serializer the sink should use.
     */
    public static final String CONFIG_SERIALIZER = "serializer";
    /**
     * Configuration to pass to the serializer.
     */
    public static final String CONFIG_SERIALIZER_PREFIX = CONFIG_SERIALIZER + ".";

    public static final String CONFIG_TIMEOUT = "timeout";

    public static final String CONFIG_ENABLE_WAL = "enableWal";

    public static final boolean DEFAULT_ENABLE_WAL = true;

    public static final long DEFAULT_TIMEOUT = 60000;

    public static final String CONFIG_KEYTAB = "kerberosKeytab";

    public static final String CONFIG_PRINCIPAL = "kerberosPrincipal";

    public static final String ZK_QUORUM = "zookeeperQuorum";

    public static final String ZK_ZNODE_PARENT = "znodeParent";

    public static final String DEFAULT_ZK_ZNODE_PARENT =
            HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT;

    public static final String CONFIG_COALESCE_INCREMENTS = "coalesceIncrements";

    public static final Boolean DEFAULT_COALESCE_INCREMENTS = false;

    public static final int DEFAULT_MAX_CONSECUTIVE_FAILS = 10;

    public static final String CONFIG_MAX_CONSECUTIVE_FAILS = "maxConsecutiveFails";
}
