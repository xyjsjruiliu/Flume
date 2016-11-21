package com.xy.lr.java.flume.source;

import com.xy.lr.java.tools.file.FindAllFileOnCatalogue;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * source
 * Created by hadoop on 11/15/16.
 */
public class FileSource extends AbstractSource implements Configurable, PollableSource {
    private String filePath;
    private List<File> currentFileList;
    private List<File> lastFileReadList;

    private ChannelProcessor channelProcessor = null;


    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        // 获取连接Channel的对象
        channelProcessor = getChannelProcessor();

        //获取当前目录新的文件
        this.currentFileList = getFileList();

        if(this.currentFileList.size() != 0) {
            try {
                Event e = EventBuilder.withBody("", Charset.forName("UTF8"));
                // 将封装后的 event发送到连接的 Channel
                channelProcessor.processEvent(e);
            } catch (Exception e) {
                // 出现错误，返回Status.BACKOFF，告诉Source 发送失败
                // 当自定义 Sink 的时候，这里需要注意，详见后面
                status = Status.BACKOFF;

//            logger.error("flume-ng file source error!", e);
                throw new EventDeliveryException(e);
            }
        }
        return status;
    }

    /**
     * 获取当前文件下文件名列表
     * @return
     */
    public List<File> getFileList() {
        FindAllFileOnCatalogue findAllFileOnCatalogue = new FindAllFileOnCatalogue();
        List<File> file = findAllFileOnCatalogue.getCatalogueList(new File(this.filePath));

        return file;
    }

    @Override
    public synchronized void start() {
        // Source 启动时的初始化、开启进程等
        this.currentFileList = new ArrayList<File>();
        this.lastFileReadList = new ArrayList<File>();
    }

    @Override
    public synchronized void stop() {
        // Source 结束时的变量释放、进程结束等

        this.currentFileList = null;
        this.lastFileReadList = null;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    /**
     *
     * @param context
     */
    public void configure(Context context) {
        this.filePath = context.getString(FileSourceContexts.FILE_PATH);
    }
}
