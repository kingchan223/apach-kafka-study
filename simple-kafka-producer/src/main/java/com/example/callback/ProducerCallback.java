package com.example.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallback implements Callback {

    private final static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

    @Override// onCompletion 메서드는 레코드의 비동기 결과를 받기 위해 사용함.
    public void onCompletion(RecordMetadata metadata, Exception e) {
        if(e != null) logger.error(e.getMessage(), e);//브로커 적재에 이슈가 생겼을 경우 Exception 에 어떤 에러가 발생하였는지 담겨서 메서드가 실행됨.
        else logger.info(metadata.toString());//에러가 발생하지 않았다면 metadata를 통해 해당 레코드가 적재된 토픽이름, 파티션 번호, 오프셋을 알 수 있음
    }
}
