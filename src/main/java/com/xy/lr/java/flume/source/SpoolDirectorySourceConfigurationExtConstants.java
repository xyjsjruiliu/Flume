package com.xy.lr.java.flume.source;

import org.apache.flume.serialization.DecodeErrorPolicy;

/**
 * Created by hadoop on 11/14/16.
 */
public class SpoolDirectorySourceConfigurationExtConstants {
    /** Directory where files are deposited. */
    public static final String SPOOL_DIRECTORY = "spoolDir";

    /** Suffix appended to files when they are finished being sent. */
    public static final String SPOOLED_FILE_SUFFIX = "fileSuffix";
    public static final String DEFAULT_SPOOLED_FILE_SUFFIX = ".COMPLETED";

    /** Header in which to put absolute path filename. */
    public static final String FILENAME_HEADER_KEY = "fileHeaderKey";
    public static final String DEFAULT_FILENAME_HEADER_KEY = "file";

    /** Whether to include absolute path filename in a header. */
    public static final String FILENAME_HEADER = "fileHeader";
    public static final boolean DEFAULT_FILE_HEADER = false;

    /** Header in which to put the basename of file. */
    public static final String BASENAME_HEADER_KEY = "basenameHeaderKey";
    public static final String DEFAULT_BASENAME_HEADER_KEY = "basename";

    /** Whether to include the basename of a file in a header. */
    public static final String BASENAME_HEADER = "basenameHeader";
    public static final boolean DEFAULT_BASENAME_HEADER = false;

    /** What size to batch with before sending to ChannelProcessor. */
    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;

    /** Maximum number of lines to buffer between commits. */
    @Deprecated
    public static final String BUFFER_MAX_LINES = "bufferMaxLines";
    @Deprecated
    public static final int DEFAULT_BUFFER_MAX_LINES = 100;

    /** Maximum length of line (in characters) in buffer between commits. */
    @Deprecated
    public static final String BUFFER_MAX_LINE_LENGTH = "bufferMaxLineLength";
    @Deprecated
    public static final int DEFAULT_BUFFER_MAX_LINE_LENGTH = 5000;

    /** Pattern of files to ignore */
    public static final String IGNORE_PAT = "ignorePattern";
    public static final String DEFAULT_IGNORE_PAT = "^$"; // no effect

    /** Directory to store metadata about files being processed */
    public static final String TRACKER_DIR = "trackerDir";
    public static final String DEFAULT_TRACKER_DIR = ".flumespool";

    /** Deserializer to use to parse the file data into Flume Events */
    public static final String DESERIALIZER = "deserializer";
    public static final String DEFAULT_DESERIALIZER = "LINE";

    public static final String DELETE_POLICY = "deletePolicy";
    public static final String DEFAULT_DELETE_POLICY = "never";

    /** Character set used when reading the input. */
    public static final String INPUT_CHARSET = "inputCharset";
    public static final String DEFAULT_INPUT_CHARSET = "UTF-8";

    /** What to do when there is a character set decoding error. */
    public static final String DECODE_ERROR_POLICY = "decodeErrorPolicy";
    public static final String DEFAULT_DECODE_ERROR_POLICY = DecodeErrorPolicy.FAIL
            .name();

    public static final String MAX_BACKOFF = "maxBackoff";

    public static final Integer DEFAULT_MAX_BACKOFF = 4000;

    // 增加代码开始
    public static final String FILENAME_EXTRACTOR = "fileExtractor";
    public static final boolean DEFAULT_FILENAME_EXTRACTOR = false;

    public static final String FILENAME_EXTRACTOR_HEADER_KEY = "fileExtractorHeaderKey";
    public static final String DEFAULT_FILENAME_EXTRACTOR_HEADER_KEY = "fileExtractorHeader";

    public static final String FILENAME_EXTRACTOR_PATTERN = "fileExtractorPattern";
    public static final String DEFAULT_FILENAME_EXTRACTOR_PATTERN = "(.)*";

    public static final String FILENAME_EXTRACTOR_CONVERT_TO_TIMESTAMP = "convertToTimestamp";
    public static final boolean DEFAULT_FILENAME_EXTRACTOR_CONVERT_TO_TIMESTAMP = false;

    public static final String FILENAME_EXTRACTOR_DATETIME_FORMAT = "dateTimeFormat";
    public static final String DEFAULT_FILENAME_EXTRACTOR_DATETIME_FORMAT = "yyyy-MM-dd";


    public static final String SPLIT_FILENAME = "splitFileName";
    public static final boolean DEFAULT_SPLIT_FILENAME = false;

    public static final String SPLITY_BY = "splitBy";
    public static final String DEFAULT_SPLITY_BY = "\\.";

    public static final String SPLIT_BASENAME_HEADER = "splitBaseNameHeader";
    public static final String DEFAULT_SPLIT_BASENAME_HEADER = "fileNameSplit";
    // 增加代码结束
}

