package com.xy.lr.java.flume.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.List;

/**
 * My HBase Sink
 * Created by hadoop on 11/14/16.
 */
public class MyHBaseSink extends AbstractSink implements Configurable {
    //HBase数据库表名
    private String tableName;
    //列族名
    private byte[] columnFamily;
    //HTable
    private HTable table;
    //batch
    private long batchSize;
    //hadoop conf
    private Configuration config;
    private static final Logger logger = LoggerFactory.getLogger(MyHBaseSink.class);
//    private HbaseEventSerializer serializer;
    private String eventSerializerType;
    private Context serializerContext;
    private String kerberosPrincipal;
    private String kerberosKeytab;
    private boolean enableWal = true;
    private boolean batchIncrements = false;
    private Method refGetFamilyMap = null;
    private SinkCounter sinkCounter;
    private PrivilegedExecutor privilegedExecutor;

    // Internal hooks used for unit testing.
    private DebugIncrementsCallback debugIncrCallback = null;

    public MyHBaseSink() {
        this(HBaseConfiguration.create());
    }

    public MyHBaseSink(Configuration conf) {
        this.config = conf;
    }

    public MyHBaseSink(Configuration conf, DebugIncrementsCallback cb) {
        this(conf);
        this.debugIncrCallback = cb;
    }

    @Override
    public void start() {
        Preconditions.checkArgument(table == null, "Please call stop " +
                "before calling start on an old instance.");
        try {
            privilegedExecutor =
                    FlumeAuthenticationUtil.getAuthenticator(kerberosPrincipal, kerberosKeytab);
        } catch (Exception ex) {
            sinkCounter.incrementConnectionFailedCount();
            throw new FlumeException("Failed to login to HBase using "
                    + "provided credentials.", ex);
        }
        try {
            table = privilegedExecutor.execute(new PrivilegedExceptionAction<HTable>() {
//                @Override
                public HTable run() throws Exception {
                    HTable table = new HTable(config, tableName);
                    table.setAutoFlush(false);
                    // Flush is controlled by us. This ensures that HBase changing
                    // their criteria for flushing does not change how we flush.
                    return table;
                }
            });
        } catch (Exception e) {
            sinkCounter.incrementConnectionFailedCount();
            logger.error("Could not load table, " + tableName +
                    " from HBase", e);
            throw new FlumeException("Could not load table, " + tableName +
                    " from HBase", e);
        }
        try {
            if (!privilegedExecutor.execute(new PrivilegedExceptionAction<Boolean>() {
//                @Override
                public Boolean run() throws IOException {
                    return table.getTableDescriptor().hasFamily(columnFamily);
                }
            })) {
                throw new IOException("Table " + tableName
                        + " has no such column family " + Bytes.toString(columnFamily));
            }
        } catch (Exception e) {
            //Get getTableDescriptor also throws IOException, so catch the IOException
            //thrown above or by the getTableDescriptor() call.
            sinkCounter.incrementConnectionFailedCount();
            throw new FlumeException("Error getting column family from HBase."
                    + "Please verify that the table " + tableName + " and Column Family, "
                    + Bytes.toString(columnFamily) + " exists in HBase, and the"
                    + " current user has permissions to access that table.", e);
        }

        super.start();
        sinkCounter.incrementConnectionCreatedCount();
        sinkCounter.start();
    }

    @Override
    public void stop() {
        try {
            if (table != null) {
                table.close();
            }
            table = null;
        } catch (IOException e) {
            throw new FlumeException("Error closing table.", e);
        }
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
    }

    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
        List<Row> actions = new LinkedList<Row>();
        List<Increment> incs = new LinkedList<Increment>();
        try {
            txn.begin();

            long i = 0;
            for (; i < batchSize; i++) {
                Event event = channel.take();
                event.getHeaders();
                if (event == null) {
                    if (i == 0) {
                        status = Status.BACKOFF;
                        sinkCounter.incrementBatchEmptyCount();
                    } else {
                        sinkCounter.incrementBatchUnderflowCount();
                    }
                    break;
                } else {
                    /*serializer.initialize(event, columnFamily);
                    actions.addAll(serializer.getActions());
                    incs.addAll(serializer.getIncrements());*/
                }
            }
            if (i == batchSize) {
                sinkCounter.incrementBatchCompleteCount();
            }
            sinkCounter.addToEventDrainAttemptCount(i);

//            putEventsAndCommit(actions, incs, txn);

        } catch (Throwable e) {
            try {
                txn.rollback();
            } catch (Exception e2) {
                logger.error("Exception in rollback. Rollback might not have been " +
                        "successful.", e2);
            }
            logger.error("Failed to commit transaction." +
                    "Transaction rolled back.", e);
            if (e instanceof Error || e instanceof RuntimeException) {
                logger.error("Failed to commit transaction." +
                        "Transaction rolled back.", e);
                Throwables.propagate(e);
            } else {
                logger.error("Failed to commit transaction." +
                        "Transaction rolled back.", e);
                throw new EventDeliveryException("Failed to commit transaction." +
                        "Transaction rolled back.", e);
            }
        } finally {
            txn.close();
        }
        return status;
    }

    @SuppressWarnings("unchecked")
//    @Override
    public void configure(Context context) {
        tableName = context.getString(HBaseSinkConfigurationConstants.CONFIG_TABLE);
        String cf = context.getString(
                HBaseSinkConfigurationConstants.CONFIG_COLUMN_FAMILY);
        batchSize = context.getLong(
                HBaseSinkConfigurationConstants.CONFIG_BATCHSIZE, new Long(100));
        serializerContext = new Context();
        //If not specified, will use HBase defaults.
        eventSerializerType = context.getString(
                HBaseSinkConfigurationConstants.CONFIG_SERIALIZER);
        Preconditions.checkNotNull(tableName,
                "Table name cannot be empty, please specify in configuration file");
        Preconditions.checkNotNull(cf,
                "Column family cannot be empty, please specify in configuration file");
        serializerContext.putAll(context.getSubProperties(
                HBaseSinkConfigurationConstants.CONFIG_SERIALIZER_PREFIX));

        kerberosKeytab = context.getString(HBaseSinkConfigurationConstants.CONFIG_KEYTAB);
        kerberosPrincipal = context.getString(HBaseSinkConfigurationConstants.CONFIG_PRINCIPAL);

        enableWal = context.getBoolean(HBaseSinkConfigurationConstants
                .CONFIG_ENABLE_WAL, HBaseSinkConfigurationConstants.DEFAULT_ENABLE_WAL);
        logger.info("The write to WAL option is set to: " + String.valueOf(enableWal));
        if (!enableWal) {
            logger.warn("HBase Sink's enableWal configuration is set to false. All " +
                    "writes to HBase will have WAL disabled, and any data in the " +
                    "memstore of this region in the Region Server could be lost!");
        }

        batchIncrements = context.getBoolean(
                HBaseSinkConfigurationConstants.CONFIG_COALESCE_INCREMENTS,
                HBaseSinkConfigurationConstants.DEFAULT_COALESCE_INCREMENTS);

        String zkQuorum = context.getString(HBaseSinkConfigurationConstants
                .ZK_QUORUM);
        Integer port = null;

        sinkCounter = new SinkCounter(this.getName());
    }

    @VisibleForTesting
    @InterfaceAudience.Private
    interface DebugIncrementsCallback {
        public void onAfterCoalesce(Iterable<Increment> increments);
    }
}
