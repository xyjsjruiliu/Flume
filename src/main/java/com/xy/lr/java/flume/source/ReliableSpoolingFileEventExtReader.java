package com.xy.lr.java.flume.source;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.serialization.DurablePositionTracker;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.apache.flume.serialization.PositionTracker;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.apache.flume.serialization.ResettableInputStream;
import org.apache.flume.tools.PlatformDetect;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

/**
 * Created by hadoop on 11/14/16.
 */
public class ReliableSpoolingFileEventExtReader implements ReliableEventReader {

    private static final Logger logger = LoggerFactory
            .getLogger(ReliableSpoolingFileEventExtReader.class);

    static final String metaFileName = ".flumespool-main.meta";

    private final File spoolDirectory;
    private final String completedSuffix;
    private final String deserializerType;
    private final Context deserializerContext;
    private final Pattern ignorePattern;
    private final File metaFile;
    private final boolean annotateFileName;
    private final boolean annotateBaseName;
    private final String fileNameHeader;
    private final String baseNameHeader;

    // 添加参数开始
    private final boolean annotateFileNameExtractor;
    private final String fileNameExtractorHeader;
    private final Pattern fileNameExtractorPattern;
    private final boolean convertToTimestamp;
    private final String dateTimeFormat;

    private final boolean splitFileName;
    private final String splitBy;
    private final String splitBaseNameHeader;
    // 添加参数结束

    private final String deletePolicy;
    private final Charset inputCharset;
    private final DecodeErrorPolicy decodeErrorPolicy;

    private Optional<FileInfo> currentFile = Optional.absent();
    /** Always contains the last file from which lines have been read. **/
    private Optional<FileInfo> lastFileRead = Optional.absent();
    private boolean committed = true;

    /**
     * Create a ReliableSpoolingFileEventReader to watch the given directory.
     */
    private ReliableSpoolingFileEventExtReader(File spoolDirectory,
                                               String completedSuffix, String ignorePattern,
                                               String trackerDirPath, boolean annotateFileName,
                                               String fileNameHeader, boolean annotateBaseName,
                                               String baseNameHeader, String deserializerType,
                                               Context deserializerContext, String deletePolicy,
                                               String inputCharset, DecodeErrorPolicy decodeErrorPolicy,
                                               boolean annotateFileNameExtractor, String fileNameExtractorHeader,
                                               String fileNameExtractorPattern, boolean convertToTimestamp,
                                               String dateTimeFormat, boolean splitFileName, String splitBy,
                                               String splitBaseNameHeader) throws IOException {

        // Sanity checks
        Preconditions.checkNotNull(spoolDirectory);
        Preconditions.checkNotNull(completedSuffix);
        Preconditions.checkNotNull(ignorePattern);
        Preconditions.checkNotNull(trackerDirPath);
        Preconditions.checkNotNull(deserializerType);
        Preconditions.checkNotNull(deserializerContext);
        Preconditions.checkNotNull(deletePolicy);
        Preconditions.checkNotNull(inputCharset);

        // validate delete policy
        if (!deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name())
                && !deletePolicy
                .equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
            throw new IllegalArgumentException("Delete policies other than "
                    + "NEVER and IMMEDIATE are not yet supported");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Initializing {} with directory={}, metaDir={}, "
                    + "deserializer={}", new Object[] {
                    ReliableSpoolingFileEventExtReader.class.getSimpleName(),
                    spoolDirectory, trackerDirPath, deserializerType });
        }

        // Verify directory exists and is readable/writable
        Preconditions
                .checkState(
                        spoolDirectory.exists(),
                        "Directory does not exist: "
                                + spoolDirectory.getAbsolutePath());
        Preconditions.checkState(spoolDirectory.isDirectory(),
                "Path is not a directory: " + spoolDirectory.getAbsolutePath());

        // Do a canary test to make sure we have access to spooling directory
        try {
            File canary = File.createTempFile("flume-spooldir-perm-check-",
                    ".canary", spoolDirectory);
            Files.write("testing flume file permissions\n", canary,
                    Charsets.UTF_8);
            List<String> lines = Files.readLines(canary, Charsets.UTF_8);
            Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s",
                    canary);
            if (!canary.delete()) {
                throw new IOException("Unable to delete canary file " + canary);
            }
            logger.debug("Successfully created and deleted canary file: {}",
                    canary);
        } catch (IOException e) {
            throw new FlumeException("Unable to read and modify files"
                    + " in the spooling directory: " + spoolDirectory, e);
        }

        this.spoolDirectory = spoolDirectory;
        this.completedSuffix = completedSuffix;
        this.deserializerType = deserializerType;
        this.deserializerContext = deserializerContext;
        this.annotateFileName = annotateFileName;
        this.fileNameHeader = fileNameHeader;
        this.annotateBaseName = annotateBaseName;
        this.baseNameHeader = baseNameHeader;
        this.ignorePattern = Pattern.compile(ignorePattern);
        this.deletePolicy = deletePolicy;
        this.inputCharset = Charset.forName(inputCharset);
        this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);

        // 增加代码开始
        this.annotateFileNameExtractor = annotateFileNameExtractor;
        this.fileNameExtractorHeader = fileNameExtractorHeader;
        this.fileNameExtractorPattern = Pattern
                .compile(fileNameExtractorPattern);
        this.convertToTimestamp = convertToTimestamp;
        this.dateTimeFormat = dateTimeFormat;

        this.splitFileName = splitFileName;
        this.splitBy = splitBy;
        this.splitBaseNameHeader = splitBaseNameHeader;
        // 增加代码结束

        File trackerDirectory = new File(trackerDirPath);

        // if relative path, treat as relative to spool directory
        if (!trackerDirectory.isAbsolute()) {
            trackerDirectory = new File(spoolDirectory, trackerDirPath);
        }

        // ensure that meta directory exists
        if (!trackerDirectory.exists()) {
            if (!trackerDirectory.mkdir()) {
                throw new IOException(
                        "Unable to mkdir nonexistent meta directory "
                                + trackerDirectory);
            }
        }

        // ensure that the meta directory is a directory
        if (!trackerDirectory.isDirectory()) {
            throw new IOException("Specified meta directory is not a directory"
                    + trackerDirectory);
        }

        this.metaFile = new File(trackerDirectory, metaFileName);
    }

    /**
     * Return the filename which generated the data from the last successful
     * {@link #readEvents(int)} call. Returns null if called before any file
     * contents are read.
     */
    public String getLastFileRead() {
        if (!lastFileRead.isPresent()) {
            return null;
        }
        return lastFileRead.get().getFile().getAbsolutePath();
    }

    // public interface
    public Event readEvent() throws IOException {
        List<Event> events = readEvents(1);
        if (!events.isEmpty()) {
            return events.get(0);
        } else {
            return null;
        }
    }

    public List<Event> readEvents(int numEvents) throws IOException {
        if (!committed) {
            if (!currentFile.isPresent()) {
                throw new IllegalStateException("File should not roll when "
                        + "commit is outstanding.");
            }
            logger.info("Last read was never committed - resetting mark position.");
            currentFile.get().getDeserializer().reset();
        } else {
            // Check if new files have arrived since last call
            if (!currentFile.isPresent()) {
                currentFile = getNextFile();
            }
            // Return empty list if no new files
            if (!currentFile.isPresent()) {
                return Collections.emptyList();
            }
        }

        EventDeserializer des = currentFile.get().getDeserializer();
        List<Event> events = des.readEvents(numEvents);

		/*
		 * It's possible that the last read took us just up to a file boundary.
		 * If so, try to roll to the next file, if there is one.
		 */
        if (events.isEmpty()) {
            retireCurrentFile();
            currentFile = getNextFile();
            if (!currentFile.isPresent()) {
                return Collections.emptyList();
            }
            events = currentFile.get().getDeserializer().readEvents(numEvents);
        }

        if (annotateFileName) {
            String filename = currentFile.get().getFile().getAbsolutePath();
            for (Event event : events) {
                event.getHeaders().put(fileNameHeader, filename);
            }
        }

        if (annotateBaseName) {
            String basename = currentFile.get().getFile().getName();
            for (Event event : events) {
                event.getHeaders().put(baseNameHeader, basename);
            }
        }

        // 增加代码开始

        // 按正则抽取文件名的内容
        if (annotateFileNameExtractor) {

            Matcher matcher = fileNameExtractorPattern.matcher(currentFile
                    .get().getFile().getName());

            if (matcher.find()) {
                String value = matcher.group();
                if (convertToTimestamp) {
                    DateTimeFormatter formatter = DateTimeFormat
                            .forPattern(dateTimeFormat);
                    DateTime dateTime = formatter.parseDateTime(value);

                    value = Long.toString(dateTime.getMillis());
                }

                for (Event event : events) {
                    event.getHeaders().put(fileNameExtractorHeader, value);
                }
            }

        }

        // 按分隔符拆分文件名
        if (splitFileName) {
            String[] splits = currentFile.get().getFile().getName()
                    .split(splitBy);
            for (Event event : events) {
                for (int i = 0; i < splits.length; i++) {
                    event.getHeaders().put(splitBaseNameHeader + i, splits[i]);
                }
            }
        }

        // 增加代码结束

        committed = false;
        lastFileRead = currentFile;
        return events;
    }

//    @Override
    public void close() throws IOException {
        if (currentFile.isPresent()) {
            currentFile.get().getDeserializer().close();
            currentFile = Optional.absent();
        }
    }

    /** Commit the last lines which were read. */
//    @Override
    public void commit() throws IOException {
        if (!committed && currentFile.isPresent()) {
            currentFile.get().getDeserializer().mark();
            committed = true;
        }
    }

    /**
     * Closes currentFile and attempt to rename it.
     *
     * If these operations fail in a way that may cause duplicate log entries,
     * an error is logged but no exceptions are thrown. If these operations fail
     * in a way that indicates potential misuse of the spooling directory, a
     * FlumeException will be thrown.
     *
     * @throws FlumeException
     *             if files do not conform to spooling assumptions
     */
    private void retireCurrentFile() throws IOException {
        Preconditions.checkState(currentFile.isPresent());

        File fileToRoll = new File(currentFile.get().getFile()
                .getAbsolutePath());

        currentFile.get().getDeserializer().close();

        // Verify that spooling assumptions hold
        if (fileToRoll.lastModified() != currentFile.get().getLastModified()) {
            String message = "File has been modified since being read: "
                    + fileToRoll;
            throw new IllegalStateException(message);
        }
        if (fileToRoll.length() != currentFile.get().getLength()) {
            String message = "File has changed size since being read: "
                    + fileToRoll;
            throw new IllegalStateException(message);
        }

        if (deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name())) {
            rollCurrentFile(fileToRoll);
        } else if (deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
            deleteCurrentFile(fileToRoll);
        } else {
            // TODO: implement delay in the future
            throw new IllegalArgumentException("Unsupported delete policy: "
                    + deletePolicy);
        }
    }

    /**
     * Rename the given spooled file
     *
     * @param fileToRoll
     * @throws IOException
     */
    private void rollCurrentFile(File fileToRoll) throws IOException {

        File dest = new File(fileToRoll.getPath() + completedSuffix);
        logger.info("Preparing to move file {} to {}", fileToRoll, dest);

        // Before renaming, check whether destination file name exists
        if (dest.exists() && PlatformDetect.isWindows()) {
			/*
			 * If we are here, it means the completed file already exists. In
			 * almost every case this means the user is violating an assumption
			 * of Flume (that log files are placed in the spooling directory
			 * with unique names). However, there is a corner case on Windows
			 * systems where the file was already rolled but the rename was not
			 * atomic. If that seems likely, we let it pass with only a warning.
			 */
            if (Files.equal(currentFile.get().getFile(), dest)) {
                logger.warn("Completed file " + dest
                        + " already exists, but files match, so continuing.");
                boolean deleted = fileToRoll.delete();
                if (!deleted) {
                    logger.error("Unable to delete file "
                            + fileToRoll.getAbsolutePath()
                            + ". It will likely be ingested another time.");
                }
            } else {
                String message = "File name has been re-used with different"
                        + " files. Spooling assumptions violated for " + dest;
                throw new IllegalStateException(message);
            }

            // Dest file exists and not on windows
        } else if (dest.exists()) {
            String message = "File name has been re-used with different"
                    + " files. Spooling assumptions violated for " + dest;
            throw new IllegalStateException(message);

            // Destination file does not already exist. We are good to go!
        } else {
            boolean renamed = fileToRoll.renameTo(dest);
            if (renamed) {
                logger.debug("Successfully rolled file {} to {}", fileToRoll,
                        dest);

                // now we no longer need the meta file
                deleteMetaFile();
            } else {
				/*
				 * If we are here then the file cannot be renamed for a reason
				 * other than that the destination file exists (actually, that
				 * remains possible w/ small probability due to TOC-TOU
				 * conditions).
				 */
                String message = "Unable to move "
                        + fileToRoll
                        + " to "
                        + dest
                        + ". This will likely cause duplicate events. Please verify that "
                        + "flume has sufficient permissions to perform these operations.";
                throw new FlumeException(message);
            }
        }
    }

    /**
     * Delete the given spooled file
     *
     * @param fileToDelete
     * @throws IOException
     */
    private void deleteCurrentFile(File fileToDelete) throws IOException {
        logger.info("Preparing to delete file {}", fileToDelete);
        if (!fileToDelete.exists()) {
            logger.warn("Unable to delete nonexistent file: {}", fileToDelete);
            return;
        }
        if (!fileToDelete.delete()) {
            throw new IOException("Unable to delete spool file: "
                    + fileToDelete);
        }
        // now we no longer need the meta file
        deleteMetaFile();
    }

    /**
     * Find and open the oldest file in the chosen directory. If two or more
     * files are equally old, the file name with lower lexicographical value is
     * returned. If the directory is empty, this will return an absent option.
     */
    private Optional<FileInfo> getNextFile() {
		/* Filter to exclude finished or hidden files */
        FileFilter filter = new FileFilter() {
            public boolean accept(File candidate) {
                String fileName = candidate.getName();
                if ((candidate.isDirectory())
                        || (fileName.endsWith(completedSuffix))
                        || (fileName.startsWith("."))
                        || ignorePattern.matcher(fileName).matches()) {
                    return false;
                }
                return true;
            }
        };
        List<File> candidateFiles = Arrays.asList(spoolDirectory
                .listFiles(filter));
        if (candidateFiles.isEmpty()) {
            return Optional.absent();
        } else {
            Collections.sort(candidateFiles, new Comparator<File>() {
                public int compare(File a, File b) {
                    int timeComparison = new Long(a.lastModified())
                            .compareTo(new Long(b.lastModified()));
                    if (timeComparison != 0) {
                        return timeComparison;
                    } else {
                        return a.getName().compareTo(b.getName());
                    }
                }
            });
            File nextFile = candidateFiles.get(0);
            try {
                // roll the meta file, if needed
                String nextPath = nextFile.getPath();
                PositionTracker tracker = DurablePositionTracker.getInstance(
                        metaFile, nextPath);
                if (!tracker.getTarget().equals(nextPath)) {
                    tracker.close();
                    deleteMetaFile();
                    tracker = DurablePositionTracker.getInstance(metaFile,
                            nextPath);
                }

                // sanity check
                Preconditions
                        .checkState(
                                tracker.getTarget().equals(nextPath),
                                "Tracker target %s does not equal expected filename %s",
                                tracker.getTarget(), nextPath);

                ResettableInputStream in = new ResettableFileInputStream(
                        nextFile, tracker,
                        ResettableFileInputStream.DEFAULT_BUF_SIZE,
                        inputCharset, decodeErrorPolicy);
                EventDeserializer deserializer = EventDeserializerFactory
                        .getInstance(deserializerType, deserializerContext, in);

                return Optional.of(new FileInfo(nextFile, deserializer));
            } catch (FileNotFoundException e) {
                // File could have been deleted in the interim
                logger.warn("Could not find file: " + nextFile, e);
                return Optional.absent();
            } catch (IOException e) {
                logger.error("Exception opening file: " + nextFile, e);
                return Optional.absent();
            }
        }
    }

    private void deleteMetaFile() throws IOException {
        if (metaFile.exists() && !metaFile.delete()) {
            throw new IOException("Unable to delete old meta file " + metaFile);
        }
    }

    /** An immutable class with information about a file being processed. */
    private static class FileInfo {
        private final File file;
        private final long length;
        private final long lastModified;
        private final EventDeserializer deserializer;

        public FileInfo(File file, EventDeserializer deserializer) {
            this.file = file;
            this.length = file.length();
            this.lastModified = file.lastModified();
            this.deserializer = deserializer;
        }

        public long getLength() {
            return length;
        }

        public long getLastModified() {
            return lastModified;
        }

        public EventDeserializer getDeserializer() {
            return deserializer;
        }

        public File getFile() {
            return file;
        }
    }

    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    static enum DeletePolicy {
        NEVER, IMMEDIATE, DELAY
    }

    /**
     * Special builder class for ReliableSpoolingFileEventReader
     */
    public static class Builder {
        private File spoolDirectory;
        private String completedSuffix = SpoolDirectorySourceConfigurationExtConstants.SPOOLED_FILE_SUFFIX;
        private String ignorePattern = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_IGNORE_PAT;
        private String trackerDirPath = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_TRACKER_DIR;
        private Boolean annotateFileName = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILE_HEADER;
        private String fileNameHeader = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_HEADER_KEY;
        private Boolean annotateBaseName = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_BASENAME_HEADER;
        private String baseNameHeader = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_BASENAME_HEADER_KEY;
        private String deserializerType = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_DESERIALIZER;
        private Context deserializerContext = new Context();
        private String deletePolicy = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_DELETE_POLICY;
        private String inputCharset = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_INPUT_CHARSET;
        private DecodeErrorPolicy decodeErrorPolicy = DecodeErrorPolicy
                .valueOf(SpoolDirectorySourceConfigurationExtConstants.DEFAULT_DECODE_ERROR_POLICY
                        .toUpperCase());

        // 增加代码开始

        private Boolean annotateFileNameExtractor = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_EXTRACTOR;
        private String fileNameExtractorHeader = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_EXTRACTOR_HEADER_KEY;
        private String fileNameExtractorPattern = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_EXTRACTOR_PATTERN;
        private Boolean convertToTimestamp = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_EXTRACTOR_CONVERT_TO_TIMESTAMP;

        private String dateTimeFormat = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_FILENAME_EXTRACTOR_DATETIME_FORMAT;

        private Boolean splitFileName = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_SPLIT_FILENAME;
        private String splitBy = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_SPLITY_BY;
        private String splitBaseNameHeader = SpoolDirectorySourceConfigurationExtConstants.DEFAULT_SPLIT_BASENAME_HEADER;

        public Builder annotateFileNameExtractor(
                Boolean annotateFileNameExtractor) {
            this.annotateFileNameExtractor = annotateFileNameExtractor;
            return this;
        }

        public Builder fileNameExtractorHeader(String fileNameExtractorHeader) {
            this.fileNameExtractorHeader = fileNameExtractorHeader;
            return this;
        }

        public Builder fileNameExtractorPattern(String fileNameExtractorPattern) {
            this.fileNameExtractorPattern = fileNameExtractorPattern;
            return this;
        }

        public Builder convertToTimestamp(Boolean convertToTimestamp) {
            this.convertToTimestamp = convertToTimestamp;
            return this;
        }

        public Builder dateTimeFormat(String dateTimeFormat) {
            this.dateTimeFormat = dateTimeFormat;
            return this;
        }

        public Builder splitFileName(Boolean splitFileName) {
            this.splitFileName = splitFileName;
            return this;
        }

        public Builder splitBy(String splitBy) {
            this.splitBy = splitBy;
            return this;
        }

        public Builder splitBaseNameHeader(String splitBaseNameHeader) {
            this.splitBaseNameHeader = splitBaseNameHeader;
            return this;
        }

        // 增加代码结束

        public Builder spoolDirectory(File directory) {
            this.spoolDirectory = directory;
            return this;
        }

        public Builder completedSuffix(String completedSuffix) {
            this.completedSuffix = completedSuffix;
            return this;
        }

        public Builder ignorePattern(String ignorePattern) {
            this.ignorePattern = ignorePattern;
            return this;
        }

        public Builder trackerDirPath(String trackerDirPath) {
            this.trackerDirPath = trackerDirPath;
            return this;
        }

        public Builder annotateFileName(Boolean annotateFileName) {
            this.annotateFileName = annotateFileName;
            return this;
        }

        public Builder fileNameHeader(String fileNameHeader) {
            this.fileNameHeader = fileNameHeader;
            return this;
        }

        public Builder annotateBaseName(Boolean annotateBaseName) {
            this.annotateBaseName = annotateBaseName;
            return this;
        }

        public Builder baseNameHeader(String baseNameHeader) {
            this.baseNameHeader = baseNameHeader;
            return this;
        }

        public Builder deserializerType(String deserializerType) {
            this.deserializerType = deserializerType;
            return this;
        }

        public Builder deserializerContext(Context deserializerContext) {
            this.deserializerContext = deserializerContext;
            return this;
        }

        public Builder deletePolicy(String deletePolicy) {
            this.deletePolicy = deletePolicy;
            return this;
        }

        public Builder inputCharset(String inputCharset) {
            this.inputCharset = inputCharset;
            return this;
        }

        public Builder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
            this.decodeErrorPolicy = decodeErrorPolicy;
            return this;
        }

        public ReliableSpoolingFileEventExtReader build() throws IOException {
            return new ReliableSpoolingFileEventExtReader(spoolDirectory,
                    completedSuffix, ignorePattern, trackerDirPath,
                    annotateFileName, fileNameHeader, annotateBaseName,
                    baseNameHeader, deserializerType, deserializerContext,
                    deletePolicy, inputCharset, decodeErrorPolicy,
                    annotateFileNameExtractor, fileNameExtractorHeader,
                    fileNameExtractorPattern, convertToTimestamp,
                    dateTimeFormat, splitFileName, splitBy, splitBaseNameHeader);
        }
    }

}

