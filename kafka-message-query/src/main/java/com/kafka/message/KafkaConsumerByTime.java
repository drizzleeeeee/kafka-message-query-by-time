package com.kafka.message;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * kafka根据时间范围拉取消息
 *
 * @author YangPengJu
 * @date 2021/11/16
 */
public class KafkaConsumerByTime {

    static KafkaConsumer<String, String> consumer;

    /**
     * 默认groupId
     */
    private static final String DEFAULT_GROUP_ID = "log_by_time";

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 拉取消息
     */
    public void pullMq() throws Exception {

        // 必填参数
        String bootstrapServers = System.getProperty("bootstrapServers");
        String topic = System.getProperty("topic");
        String fetchStartTimeStr = System.getProperty("fetchStartTime");
        String fetchEndTimeStr = System.getProperty("fetchEndTime");

        // 非必填参数
        String groupId = System.getProperty("groupId");
        String filterValue = System.getProperty("filterValue");

        // 必填参数校验
        if (StringUtils.isBlank(bootstrapServers)) {
            System.err.println("服务器地址列表参数不能为空！");
            return;
        }
        if (StringUtils.isBlank(topic)) {
            System.err.println("topic参数不能为空！");
            return;
        }
        if (StringUtils.isBlank(fetchStartTimeStr)) {
            System.err.println("统计开始时间参数不能为空！");
            return;
        }
        if (StringUtils.isBlank(fetchEndTimeStr)) {
            System.err.println("统计结束时间参数不能为空！");
            return;
        }

        // 非必填参数处理
        if (StringUtils.isBlank(groupId)) {
            System.out.println("groupId为空，使用默认groupId:" + DEFAULT_GROUP_ID);
            groupId = DEFAULT_GROUP_ID;
        }


        long fetchStartTime = 0;
        long fetchEndTime = 0;
        try {
            fetchStartTime = dateToStamp(fetchStartTimeStr);
            fetchEndTime = dateToStamp(fetchEndTimeStr);
        } catch (ParseException e) {
            throw new Exception(e.toString());
        }
        if (fetchStartTime == 0 || fetchEndTime == 0) {
            throw new Exception("startTime|endTime error!");
        }
        if (fetchStartTime > fetchEndTime) {
            throw new Exception("error! startTime is greater than endTime.");
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        System.out.println("props:" + props);
        consumer = new KafkaConsumer<>(props);

        // 根据时间段及过滤条件获取指定数据
        getMsgByTime(topic, fetchStartTime, fetchEndTime, filterValue);
        finish();
        System.out.println("finish!");
    }

    // 钩子函数，子类实现业务
    public void finish() {
    }

    /**
     * 根据时间范围和关键词查询消息
     */
    private void getMsgByTime(String topic, long fetchStartTime, long fetchEndTime, String filterValue) throws IOException {
        // 从分区到要查找的时间戳的映射
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        // 获取有关给定主题的分区的元数据
        List<PartitionInfo> partitions = consumer.partitionsFor(topic);
        for (PartitionInfo par : partitions) {
            timestampsToSearch.put(new TopicPartition(topic, par.partition()), fetchStartTime);
        }

        // 从分区到时间戳和偏移量的映射
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = consumer.offsetsForTimes(timestampsToSearch);

        //遍历每个分区，将不同分区的数据写入不同文件中
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : topicPartitionOffsetAndTimestampMap.entrySet()) {
            TopicPartition key = entry.getKey();
            OffsetAndTimestamp value = entry.getValue();
            System.out.println("分区：" + key + "开始过滤范围" + value);

            // 如果分区值不为空，说明该分区无符合条件的消息，处理下一个分区
            if (Objects.isNull(value)) {
                continue;
            }

            // 重置要读的分区offset位置
            long offset = value.offset();
            consumer.assign(Collections.singletonList(key));
            consumer.seek(key, offset);

            //拉取消息
            while (true) {
                ConsumerRecords<String, String> poll = consumer.poll(1000);

                // 如果拉取不到消息，说明已经拉取完毕
                if (poll.isEmpty()) {
                    break;
                }

                StringBuilder stringBuilder = new StringBuilder(20000);
                for (ConsumerRecord<String, String> record : poll) {
                    // 如果消息时间戳已经大于最大时间戳，后续时间的消息不在过滤范围内，不再拉取
                    if (record.timestamp() > fetchEndTime) {
                        break;
                    }

                    // 如果不满足过滤条件，跳过，处理下一条消息
                    if (StringUtils.isNotBlank(filterValue) && !record.value().contains(filterValue)) {
                        continue;
                    }
                    bussinessProcess(stringBuilder, record);
                    stringBuilder.append(record.timestamp() + "      " + stampToDate(record.timestamp()) + "      " + record.value() + "\r\n");
                    // 消息写到文件中
                    FileUtils.write(new File("./" + topic + "/" + "log_" + key.toString() + ".txt"), stringBuilder.toString(), StandardCharsets.UTF_8, true);
                    stringBuilder.setLength(0);
                }
            }
        }
    }

    /**
     * 钩子函数，子类实现自定义业务
     */
    public void bussinessProcess(StringBuilder stringBuilder, ConsumerRecord<String, String> record) {
    }

    /**
     * 将时间转换为时间戳
     */
    private static Long dateToStamp(String s) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);
        Date date = simpleDateFormat.parse(s);
        return date.getTime();
    }

    /**
     * 将时间转换为时间戳
     */
    private static String stampToDate(long timeStamp) {
        Date date = new Date(timeStamp);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);
        return simpleDateFormat.format(date);
    }
}