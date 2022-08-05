package com.example.kafkaspringproducerwithrestcontroller.pipeline;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.Date;

@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*")
public class ProduceController {

    private final Logger logger = LoggerFactory.getLogger(ProduceController.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    // 카프카 스프링의 kafkaTemplate 인스턴스를 생성
    // 메시지 키와 메시지 값은 String 타입으로 설정.
    public ProduceController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/api/select")
    public void selectColor(
            @RequestHeader("user-agent") String userAgentName,//user-agent 는 브라우저에서 호출 시 자동으로 들어가늦ㄴ 헤더 값임
            @RequestParam(value = "color")String colorName,
            @RequestParam(value = "user")String userName) {
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
        Date now = new Date();
        Gson gson = new Gson();
        UserEventVO userEventVO = new UserEventVO(sdfDate.format(now), userAgentName, colorName, userName);
        String jsonColorLog = gson.toJson(userEventVO);
        kafkaTemplate.send("select-color", jsonColorLog)//현재 메시지 키를 지정하지 않으므로 send() 메서드에는 토픽과 메시지 값만 넣으면 된다.
                .addCallback(// 데이터가 정상적으로 전송되었는지 확인하기 위해 addCallback() 메서드를 붙인다.
                        new ListenableFutureCallback<>() {
                            @Override
                            public void onSuccess(SendResult<String, String> result) {
                                logger.info(result.toString());
                            }

                            @Override
                            public void onFailure(Throwable ex) {
                                logger.error(ex.getMessage(), ex);
                            }
                    });
    }
}