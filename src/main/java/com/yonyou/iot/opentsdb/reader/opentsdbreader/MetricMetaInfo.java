package com.yonyou.iot.opentsdb.reader.opentsdbreader;

import java.util.List;

/**
 * @author huaxin
 * @date 2022/7/7 15:08 Copyright @ 用友网络科技股份有限公司
 */
public class MetricMetaInfo {
    private List<String> header;
    private List<Content> content;

    public static class Content {
        private String metric;
        private String type;
        private List<String> tags;

        public String getMetric() {
            return metric;
        }

        public void setMetric(String metric) {
            this.metric = metric;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }
    }

    public List<String> getHeader() {
        return header;
    }

    public void setHeader(List<String> header) {
        this.header = header;
    }

    public List<Content> getContent() {
        return content;
    }

    public void setContent(List<Content> content) {
        this.content = content;
    }
}
