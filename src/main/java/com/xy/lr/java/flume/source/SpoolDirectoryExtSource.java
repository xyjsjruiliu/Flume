package com.xy.lr.java.flume.source;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.LineDeserializer;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Created by hadoop on 11/14/16.
 */
public class SpoolDirectoryExtSource extends AbstractSource implements
        Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory
            .getLogger(SpoolDirectoryExtSource.class);

    // Delay used when polling for new files
    private static final int POLL_DELAY_MS = 500;

    /* Config options */
    private String completedSuffix;
    private String spoolDirectory;
    private boolean fileHeader;
    private String fileHeaderKey;
    private boolean basenameHeader;
    private String basenameHeaderKey;
    private int batchSize;
    private String ignorePattern;
    private String trackerDirPath;
    private String deserializerType;
    private Context deserializerContext;
    private String deletePolicy;
    private String inputCharset;
    private DecodeErrorPolicy decodeErrorPolicy;
    private volatile boolean hasFatalError = false;

    private SourceCounter sourceCounter;
    ReliableSpoolingFileEventExtReader reader;
    private ScheduledExecutorService executor;
    private boolean backoff = true;
    private boolean hitChannelException = false;
    private int maxBackoff;

    // 增加代码开始

    private Boolean annotateFileNameExtractor;
    private String fileNameExtractorHeader;
    private String fileNameExtractorPattern;
    private Boolean convertToTimestamp;

    private String dateTimeFormat;

    private boolean splitFileName;
    private  String splitBy;
    private  String splitBaseNameHeader;

    // 增加代码结束

    @Override
    public synchronized void start() {
        logger.info("SpoolDirectorySource source starting with directory: {}",
                spoolDirectory);

        executor = Executors.newSingleThreadScheduledExecutor();

        File directory = new File(spoolDirectory);
        try {
            reader = new ReliableSpoolingFileEventExtReader.Builder()
                    .spoolDirectory(directory).completedSuffix(completedSuffix)
                    .ignorePattern(ignorePattern)
                    .trackerDirPath(trackerDirPath)
                    .annotateFileName(fileHeader).fileNameHeader(fileHeaderKey)
                    .annotateBaseName(basenameHeader)
                    .baseNameHeader(basenameHeaderKey)
                    .deserializerType(deserializerType)
                    .deserializerContext(deserializerContext)
                    .deletePolicy(deletePolicy).inputCharset(inputCharset)
                    .decodeErrorPolicy(decodeErrorPolicy)
                    .annotateFileNameExtractor(annotateFileNameExtractor)
                    .fileNameExtractorHeader(fileNameExtractorHeader)
                    .fileNameExtractorPattern(fileNameExtractorPattern)
                    .convertToTimestamp(convertToTimestamp)
                    .dateTimeFormat(dateTimeFormat)
                    .splitFileName(splitFileName).splitBy(splitBy)
                    .splitBaseNameHeader(splitBaseNameHeader).build();
        } catch (IOException ioe) {
            throw new FlumeException(
                    "Error instantiating spooling event parser", ioe);
        }

        Runnable runner = new SpoolDirectoryRunnable(reader, sourceCounter);
        executor.scheduleWithFixedDelay(runner, 0, POLL_DELAY_MS,
                TimeUnit.MILLISECONDS);

        super.start();
        logger.debug("SpoolDirectorySource source started");
        sourceCounter.start();
    }

    @Override
    public synchronized void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            logger.info("Interrupted while awaiting termination", ex);
        }
        executor.shutdownNow();

        super.stop();
        sourceCounter.stop();
        logger.info("SpoolDir source {} stopped. Metrics: {}", getName(),
                sourceCounter);
    }

    @Override
    public String toString() {
        return "Spool Directory source " + getName() + ": { spoolDir: "
                + spoolDirectory + " }";
    }

//    @Override
    public synchronized void configure(Context context) {
        spoolDirectory = context.getString(SpoolDirectorySourceConfigurationExtConstants.SPOOL_DIRECTORY);
        Preconditions.checkState(spoolDirectory != null,
                "Configuration must specify a spooling directory");

        completedSuffix = context.getString(SpoolDirectorySourceConfigurationExtConstants.SPOOLED_FILE_SUFFIX,
                SpoolDirectorySourceConfigurationExtConstants.DEFAULT_SPOOLED_FILE_SUFFIX);
        deletePolicy = context.getString(SpoolDirectorySourceConfigurationExtConstants.DELETE_POLICY, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_DELETE_POLICY);
        fileHeader = context.getBoolean(SpoolDirectorySourceConfigurationExtConstants.FILENAME_HEADER, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILE_HEADER);
        fileHeaderKey = context.getString(SpoolDirectorySourceConfigurationExtConstants.FILENAME_HEADER_KEY,
                SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_HEADER_KEY);
        basenameHeader = context.getBoolean(SpoolDirectorySourceConfigurationExtConstants.BASENAME_HEADER,
                SpoolDirectorySourceConfigurationExtConstants.DEFAULT_BASENAME_HEADER);
        basenameHeaderKey = context.getString(SpoolDirectorySourceConfigurationExtConstants.BASENAME_HEADER_KEY,
                SpoolDirectorySourceConfigurationExtConstants.DEFAULT_BASENAME_HEADER_KEY);
        batchSize = context.getInteger(SpoolDirectorySourceConfigurationExtConstants.BATCH_SIZE, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_BATCH_SIZE);
        inputCharset = context.getString(SpoolDirectorySourceConfigurationExtConstants.INPUT_CHARSET, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_INPUT_CHARSET);
        decodeErrorPolicy = DecodeErrorPolicy
                .valueOf(context.getString(SpoolDirectorySourceConfigurationExtConstants.DECODE_ERROR_POLICY,
                        SpoolDirectorySourceConfigurationExtConstants.DEFAULT_DECODE_ERROR_POLICY).toUpperCase());

        ignorePattern = context.getString(SpoolDirectorySourceConfigurationExtConstants.IGNORE_PAT, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_IGNORE_PAT);
        trackerDirPath = context.getString(SpoolDirectorySourceConfigurationExtConstants.TRACKER_DIR, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_TRACKER_DIR);

        deserializerType = context
                .getString(SpoolDirectorySourceConfigurationExtConstants.DESERIALIZER, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_DESERIALIZER);
        deserializerContext = new Context(context.getSubProperties(SpoolDirectorySourceConfigurationExtConstants.DESERIALIZER
                + "."));

        // "Hack" to support backwards compatibility with previous generation of
        // spooling directory source, which did not support deserializers
        Integer bufferMaxLineLength = context
                .getInteger(SpoolDirectorySourceConfigurationExtConstants.BUFFER_MAX_LINE_LENGTH);
        if (bufferMaxLineLength != null && deserializerType != null
                && deserializerType.equalsIgnoreCase(SpoolDirectorySourceConfigurationExtConstants.DEFAULT_DESERIALIZER)) {
            deserializerContext.put(LineDeserializer.MAXLINE_KEY,
                    bufferMaxLineLength.toString());
        }

        maxBackoff = context.getInteger(SpoolDirectorySourceConfigurationExtConstants.MAX_BACKOFF, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_MAX_BACKOFF);
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

        //增加代码开始

        annotateFileNameExtractor=context.getBoolean(SpoolDirectorySourceConfigurationExtConstants.FILENAME_EXTRACTOR, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_EXTRACTOR);
        fileNameExtractorHeader=context.getString(SpoolDirectorySourceConfigurationExtConstants.FILENAME_EXTRACTOR_HEADER_KEY, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_EXTRACTOR_HEADER_KEY);
        fileNameExtractorPattern=context.getString(SpoolDirectorySourceConfigurationExtConstants.FILENAME_EXTRACTOR_PATTERN, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_EXTRACTOR_PATTERN);
        convertToTimestamp=context.getBoolean(SpoolDirectorySourceConfigurationExtConstants.FILENAME_EXTRACTOR_CONVERT_TO_TIMESTAMP, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_EXTRACTOR_CONVERT_TO_TIMESTAMP);
        dateTimeFormat=context.getString(SpoolDirectorySourceConfigurationExtConstants.FILENAME_EXTRACTOR_DATETIME_FORMAT, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_EXTRACTOR_DATETIME_FORMAT);

        splitFileName=context.getBoolean(SpoolDirectorySourceConfigurationExtConstants.SPLIT_FILENAME, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_SPLIT_FILENAME);
        splitBy=context.getString(SpoolDirectorySourceConfigurationExtConstants.SPLITY_BY, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_SPLITY_BY);
        splitBaseNameHeader=context.getString(SpoolDirectorySourceConfigurationExtConstants.SPLIT_BASENAME_HEADER, SpoolDirectorySourceConfigurationExtConstants.DEFAULT_SPLIT_BASENAME_HEADER);



        //增加代码结束
    }

    @VisibleForTesting
    protected boolean hasFatalError() {
        return hasFatalError;
    }

    /**
     * The class always backs off, this exists only so that we can test without
     * taking a really long time.
     *
     * @param backoff
     *            - whether the source should backoff if the channel is full
     */
    @VisibleForTesting
    protected void setBackOff(boolean backoff) {
        this.backoff = backoff;
    }

    @VisibleForTesting
    protected boolean hitChannelException() {
        return hitChannelException;
    }

    @VisibleForTesting
    protected SourceCounter getSourceCounter() {
        return sourceCounter;
    }

    private class SpoolDirectoryRunnable implements Runnable {
        private ReliableSpoolingFileEventExtReader reader;
        private SourceCounter sourceCounter;

        public SpoolDirectoryRunnable(
                ReliableSpoolingFileEventExtReader reader,
                SourceCounter sourceCounter) {
            this.reader = reader;
            this.sourceCounter = sourceCounter;
        }

//        @Override
        public void run() {
            int backoffInterval = 250;
            try {
                while (!Thread.interrupted()) {
                    List<Event> events = reader.readEvents(batchSize);
                    if (events.isEmpty()) {
                        break;
                    }
                    sourceCounter.addToEventReceivedCount(events.size());
                    sourceCounter.incrementAppendBatchReceivedCount();

                    try {
                        getChannelProcessor().processEventBatch(events);
                        reader.commit();
                    } catch (ChannelException ex) {
                        logger.warn("The channel is full, and cannot write data now. The "
                                + "source will try again after "
                                + String.valueOf(backoffInterval)
                                + " milliseconds");
                        hitChannelException = true;
                        if (backoff) {
                            TimeUnit.MILLISECONDS.sleep(backoffInterval);
                            backoffInterval = backoffInterval << 1;
                            backoffInterval = backoffInterval >= maxBackoff ? maxBackoff
                                    : backoffInterval;
                        }
                        continue;
                    }
                    backoffInterval = 250;
                    sourceCounter.addToEventAcceptedCount(events.size());
                    sourceCounter.incrementAppendBatchAcceptedCount();
                }
                logger.info("Spooling Directory Source runner has shutdown.");
            } catch (Throwable t) {
                logger.error(
                        "FATAL: "
                                + SpoolDirectoryExtSource.this.toString()
                                + ": "
                                + "Uncaught exception in SpoolDirectorySource thread. "
                                + "Restart or reconfigure Flume to continue processing.",
                        t);
                hasFatalError = true;
                Throwables.propagate(t);
            }
        }
    }
}

