package com.silence.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


public class Flink_CDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DebeziumSourceFunction<String> build = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .password("000000")
                .username("root")
                .databaseList("flinkcdc")
                .tableList("flinkcdc.test")
                .startupOptions(StartupOptions.initial())
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        String topic = sourceRecord.topic();
                        String[] split = topic.split("\\.");
                        String db=split[1];
                        String table=split[2];
                        //获取操作类型 READ DELETE UPDATE CREATE
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        if(!operation.toString().equals("DELETE")){
                            Struct value = (Struct)sourceRecord.value();
                            Struct after = value.getStruct("after");
                            JSONObject data=new JSONObject();
                            data.put("op",operation);
                            data.put("db",db);
                            data.put("tb",table);
                            for(Field f:after.schema().fields()){
                                Object o = after.get(f);
                                data.put(f.name(),o);
                            }
                            collector.collect(data.toString());
                        }

                    }

                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        DataStreamSource<String> ds = env.addSource(build);
        ds.print();
        env.execute("cdc_schema");
    }
}
