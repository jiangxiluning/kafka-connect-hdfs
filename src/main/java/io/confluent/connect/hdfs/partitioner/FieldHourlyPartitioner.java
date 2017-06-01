package io.confluent.connect.hdfs.partitioner;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.PartitionException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Map;


/**
 * Created by luning on 2017/6/1.
 */
public class FieldHourlyPartitioner extends HourlyPartitioner implements Partitioner {
    private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);
    private static String fieldName;


    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        Schema valueSchema = sinkRecord.valueSchema();
        long timestamp;
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            Object partitionKey = struct.get(fieldName);
            Schema.Type type = valueSchema.field(fieldName).schema().type();
            switch (type) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                    timestamp = ((Number) partitionKey).longValue();
                    break;
                case STRING:
                    String timestampStr = (String) partitionKey;
                    timestamp =  Long.valueOf(timestampStr).longValue();
                    break;
                default:
                    log.error("Type {} is not supported as a partition key.", type.getName());
                    throw new PartitionException("Error encoding partition.");
            }
        } else {
            log.error("Value is not Struct type.");
            throw new PartitionException("Error encoding partition.");
        }

        DateTime bucket = new DateTime(getPartition(partitionDurationMs, timestamp, formatter.getZone()));
        return bucket.toString(formatter);
    }

    @Override
    public void configure(Map<String, Object> config) {
        super.configure(config);
        fieldName = (String) config.get(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG);
    }
}
