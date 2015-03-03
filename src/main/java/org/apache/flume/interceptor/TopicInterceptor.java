package org.apache.flume.interceptor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TopicInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(TopicInterceptor.class);
    // property : filepath = topic
    private volatile Properties prop   = new Properties();
    private String              configUri;
    private String              group;
    private long                checkPeriod;
    private Timer               timer;

    public TopicInterceptor(String configUri, String group, long checkPeriod) {
        this.configUri = configUri;
        this.group = group;
        this.checkPeriod = checkPeriod;
    }

    @Override
    public void initialize() {
        loadProp();
        timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                loadProp();
            }
        }, checkPeriod, checkPeriod);
    }

    @Override
    public Event intercept(Event event) {
        String t = event.getHeaders().get("topic");
        event.getHeaders().put("topic", prop.getProperty(t, t));
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {}

    private void loadProp() {
        Properties properties = new Properties();
        String filePath = configUri + group;
        InputStream inStream = null;
        try {
            if (filePath.startsWith("file://")) {
                filePath = filePath.substring("file://".length());
                inStream = getFileAsStream(filePath);
            } else if (filePath.startsWith("http://") || filePath.startsWith("https://")) {
                URL url = new URL(filePath);
                inStream = url.openStream();
            } else {
                inStream = getFileAsStream(filePath);
            }
            if (inStream == null) {
                logger.error("load config file error, file : " + filePath);
            }
            properties.load(inStream);
            prop = properties;
        } catch (Exception ex) {
            logger.error("load config file error, file : " + filePath, ex);
        } finally {
            if (inStream != null) {
                try {
                    inStream.close();
                } catch (Exception e) {
                    logger.error("close error", e);
                }
            }
        }
        System.out.println(prop.toString());// TODO
    }

    private InputStream getFileAsStream(String filePath) throws FileNotFoundException {
        InputStream inStream = null;
        File file = new File(filePath);
        if (file.exists()) {
            inStream = new FileInputStream(file);
        } else {
            inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
        }
        return inStream;
    }

    public static class Builder implements Interceptor.Builder {

        private String configUri;
        private String group;
        private long   checkPeriod;

        @Override
        public void configure(Context context) {
            this.configUri = context.getString("configUri");
            Preconditions.checkState(this.configUri != null, "The parameter configUri must be specified");
            this.group = context.getString("group");
            Preconditions.checkState(this.group != null, "The parameter group must be specified");
            this.checkPeriod = context.getLong("checkPeriod", 600000L);
        }

        @Override
        public Interceptor build() {
            logger.info(String.format("Creating TopicInterceptor: "));
            return new TopicInterceptor(this.configUri, group, checkPeriod);
        }
    }
}
