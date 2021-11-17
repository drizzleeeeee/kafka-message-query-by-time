package com.kafka.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 拉取消息示例
 *
 * @author YangPengJu
 * @date 2021/11/17
 */
public class ImKafkaMqQuery extends KafkaConsumerByTime {

    /**
     * 美团推送消息中的秒级时间戳字段名
     */
    private static final String CTS = "cts";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Integer TIME_STAMP_LENGTH = 10;

    // 超时时长报警，单位毫秒
    private static Long maxDelayMillSeconds;

    AtomicLong msgCount = new AtomicLong(0);
    AtomicLong threeSecondsDelayCount = new AtomicLong(0);

    AtomicLong twentySecondsDelayCount = new AtomicLong(0);

    public static void main(String[] args) throws Exception {

        // 超时时长报警，单位毫秒
        String maxDelayMillSecondsStr = System.getProperty("maxDelayMillSeconds");
        if (StringUtils.isNotBlank(maxDelayMillSecondsStr)) {
            maxDelayMillSeconds = Long.parseLong(maxDelayMillSecondsStr);
        }

        ImKafkaMqQuery imKafkaMqQuery = new ImKafkaMqQuery();
        imKafkaMqQuery.pullMq();
    }

    @Override
    public void bussinessProcess(StringBuilder stringBuilder, ConsumerRecord<String, String> record) {
        try {
            msgCount.incrementAndGet();
            Map map = OBJECT_MAPPER.readValue(record.value(), Map.class);
            String cts = String.valueOf(map.get(CTS));
            if (StringUtils.isBlank(cts) || cts.length() != TIME_STAMP_LENGTH) {
                stringBuilder.append("cts格式错误:").append(cts).append("\r\n");
                return;
            }
            long millSeconds = Long.parseLong(cts) * 1000;
            long delayMillSeconds = record.timestamp() - millSeconds;

            if (delayMillSeconds > 3000) {
                threeSecondsDelayCount.incrementAndGet();
            }

            if (delayMillSeconds > 20000) {
                twentySecondsDelayCount.incrementAndGet();
            }

            // 如果消息延时超过指定数值，打印报警信息
            if (Objects.nonNull(maxDelayMillSeconds) && delayMillSeconds > maxDelayMillSeconds) {
                stringBuilder.append("消息超时，超时时长").append(delayMillSeconds).append("\r\n");
            } else {
                stringBuilder.append("消息延时").append(delayMillSeconds).append("\r\n");
            }
        } catch (Exception e) {
            stringBuilder.append(e.toString()).append("\r\n");
            e.printStackTrace();
        }
    }


    @Override
    public void finish() {
        System.out.println("3秒消息" + threeSecondsDelayCount);
        System.out.println("20s消息：" + twentySecondsDelayCount);
        System.out.println("总消息:" + msgCount);
    }
}