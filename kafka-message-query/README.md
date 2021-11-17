使用命令示例：

｜bootstrapServers｜kafka服务器地址列表，多个服务器用逗号分隔｜
｜topic｜要拉取的话题｜
｜fetchStartTime｜消息开始时间，格式 yyyy-MM-dd HH:mm:ss｜
｜fetchEndTime｜消息结束时间，格式 yyyy-MM-dd HH:mm:ss｜
｜maxDelayMillSeconds｜超时告警｜

java -jar -DbootstrapServers='' -Dtopic='ish_im_mt_msg' -DfetchStartTime='2021-11-01 00:00:00' -DfetchEndTime='2021-11-16 21:30:30' -DmaxDelayMillSeconds='3000' kafka-message-query-1.0-SNAPSHOT.jar