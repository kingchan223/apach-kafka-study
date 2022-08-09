package com.pipeline;

import com.google.gson.Gson;
import com.pipeline.config.ElasticSearchSinkConnectorConfig;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class ElasticSearchSinkTask extends SinkTask {
    private final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkTask.class);


    private ElasticSearchSinkConnectorConfig config;
    private RestHighLevelClient esClient;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            config = new ElasticSearchSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException(e.getMessage(), e);
        }

        // 엘라스틱서치에 적재하기 위해 RestHighLevelClient 인스턴스를 생성한다.
        // 사용자가 입력한 호스트와 포트를 기반으로 RestHighLevelClient 인스턴스를 생성한다.
        esClient = new RestHighLevelClient(
                RestClient
                        .builder(
                                new HttpHost(config.getString(config.ES_CLUSTER_HOST),
                                config.getInt(config.ES_CLUSTER_PORT)))
                        );
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if(records.size() > 0){//레코드가 1개 이상, 0개 초과로 들어올 경우 엘라스틱 서치로 전송하기 위한 BulkRequest 생성
            BulkRequest bulkRequest = new BulkRequest();//BulkRequest: 1개 이상의 데이터들을 묶음으로 엘라스틱 서치로 전송할 때 사용
            for (SinkRecord record : records) {//레코드들을 BulkRequest 에 추가
                Gson gson = new Gson();
                Map map = gson.fromJson(record.value().toString(), Map.class);//토픽의 메시지 값이 JSON String이므로 Gson으로 JSON을 Map으로 변환
                bulkRequest.add(new IndexRequest(config.getString(config.ES_INDEX))//BulkRequest 에 데이터를 추가할 때는 Map 타입의 데이터와 인덱스 이름이 필요
                        .source(map, XContentType.JSON));
                logger.info("record : {}", record.value());
            }

            //bulkRequest에 담은 데이터들을 bulkAsync() 메서드로 전송 bulkAsync()f를 사용하면 데이터를 전송하고 비동기로 받아서 확인 가능
            esClient.bulkAsync(
                        bulkRequest,
                        RequestOptions.DEFAULT,
                        new ActionListener<BulkResponse>() {//엘라스틱서치에 데이터가 제대로 담겼는지 여부를 로그로 남긴다.
                            @Override
                            public void onResponse(BulkResponse bulkResponse) {
                                if (bulkResponse.hasFailures()) {
                                    logger.error(bulkResponse.buildFailureMessage());
                                } else {
                                    logger.info("bulk save success");
                                }
                            }

                            @Override
                                public void onFailure(Exception e) {
                                    logger.error(e.getMessage(), e);
                                }
                        }
                    );
        }
    }

    //flush() 메서드는 일정 주기마다 호출된다.
    //여기서는 put() 메서드에서 레코드들을 받아서 엘라스틱서치에 데이터 전송하므로 flush()에 추가 작성 X
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        logger.info("flush");
    }

    // 커넥터가 종료될 경우 엘라스틱 서치와 연동하는 esClient 변수를 안전하게 종료
    @Override
    public void stop() {
        try {
            esClient.close();
        } catch (IOException e) {
            logger.info(e.getMessage(), e);
        }
    }
}