# java-kafka-consumer
use java high level api, can consumer message from different kafka cluster

## compile
```
mvn assembly:assembly
```
## configure
+ There should be two configure files, one for kafka clusters, the other for message filter(if you need to filter message by some tag)
+ You can configure different kafka cluster in the servers.xml include zookeeper addr, group id, topic and how many threads would to consumer the topic message(the thread number should not bigger than the partition number)
+ If you want to filter some message from kafka, you can config the whitelist.xml, this project provides a default decoder to decode formated message,the format like this: `content=xxx1&type=xxx2` or `type=xxx1&content=xxx2`, which message's type is in the whitelist.xml would be write to file.
+ You can storage the message both in local filesystem and hdfs, the writer will create new file to storage the message peroid(default 60s),you can configure this by call a constructor with `interval parm` . In fact, the HdfsWriter and LocalWriter is total independent. You can use it in your own project.

