package com.pipeline.consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/*
* ConsumerWorker는 HdfsSinkApplication에서 전달받은 Properties 인스턴스로 컨슈머를 생성, 실행하는 스레드 클래스임.
* 토픽에서 데이터를 받아 HDFS에 메시지 값들을 저장한다.
* */
//Consumer 가 실행될 스레드를 정의하기 위해 Runnable 인터페이스로 ConsumerWorker 클래스 구현
//ConsumerWorker 클래스를 스레드로 실행하면 오버라이드된 run()메서드가 수행됨.
public class ConsumerWorker implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);

    //컨슈머의 poll 메서드를 통해 전달받은 데이터를 임시저장하는 버퍼. 다수 스레드가 접근할 수 있으므로 ConcurrentHashMap 사용
    //bufferString 에는 파티션 번호와 메시지 값들이 들어간다.
    private static Map<Integer, List<String>> bufferString = new ConcurrentHashMap<>();

    // currentFileOffset 은 오프셋값을 저장하고 파일 이름을 저장할 때 오프셋 번호를 붙이는 데에 사용된다.
    private static Map<Integer, Long> currentFileOffset = new ConcurrentHashMap<>();

    private final static int FLUSH_RECORD_COUNT = 10;
    private Properties prop;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker(Properties prop, String topic, int number) {
        logger.info("Generate ConsumerWorker");
        this.prop = prop;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;//스레드에 이름을 붙여 로깅시에 편하게 함.
    }

    @Override
    public void run() {
        Thread.currentThread().setName(this.threadName);

        consumer = new KafkaConsumer<>(this.prop);//스레드를 생성하는 HdfsSinkApplication 에서 설정한 컨슈머 설정을 가져와서KafkaConsumer 인스턴스를 생성.
        consumer.subscribe(Arrays.asList(this.topic));//토픽을 구독

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    addHdfsFileBuffer(record);//poll 메서드로 가져온 데이터들을 버퍼에 쌓기 위해 addHdfsFileBuffer 호출
                }
                // 한번 polling 이 완료되고 버퍼에 쌓임 ==>
                // saveBufferToHdfsFile 을 호출해서 버퍼에 쌓인 데이터 일정 개수 도달 ==>
                // HDFS 에 저장.
                saveBufferToHdfsFile(consumer.assignment());//consumer.assignment()를 인자로 넘겨주는 것은
                // 현재 컨슈머 스레드에 할당된 파티션에 대한 버퍼 데이터만 적재할 수 있도록 하기 위함
            }
        } catch (WakeupException e) {//안ㅓㄴ한 종료를 위해 WakeUpException을 받아서 컨슈머 종료 과정 수행
            logger.warn("Wakeup consumer");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    // 레코드를 받아서 메시지 값을 버퍼에 넣는 코드.
    private void addHdfsFileBuffer(ConsumerRecord<String, String> record) {
        List<String> buffer = bufferString.getOrDefault(record.partition(), new ArrayList<>());
        buffer.add(record.value());
        bufferString.put(record.partition(), buffer);

        if (buffer.size() == 1)//만약 버퍼의 크기가 1이라면 버퍼의 가장 처음 오프셋이라는 뜻이므로 currentFileOffset 변수에 넣는다.
            currentFileOffset.put(record.partition(), record.offset());
        // 위처럼 currentFileOffset 변수로 오프셋 번호를 관리하면 추후 파일을 저장할 때 파티션 이름과 오프셋 번호를
        // 붙여서 저장할 수 있기에 이슈 발생 시 파티션, 오프셋에 대한 정보를 알 수 있다.
    }

    //버퍼의 데이터가 flush 될 만큼 개수가 충족되었는지 확인하는 checkFlushCount 메서드 호출.
    //컨슈머로부터 Set<TopicPartition> 정보를 받아서 컨슈머 스레드에 할당된 파티션에만 접근한다.
    private void saveBufferToHdfsFile(Set<TopicPartition> partitions) {
        partitions.forEach(p -> checkFlushCount(p.partition()));
    }

    // 파티션 번호의 버퍼를 확인 -> flush 수행할만큼 레코드 개수가 찾는지 확인.
    // 일정 개수 이상이면 HDFS 적재 로직인 save() 메서드 호출
    // (현재 FLUSH_RECORD_COUNT == 10 이므로 10개 이상의 데이터가 쌓이면 save() 호출)
    private void checkFlushCount(int partitionNo) {
        if (bufferString.get(partitionNo) != null) {
            if (bufferString.get(partitionNo).size() > FLUSH_RECORD_COUNT - 1) {
                save(partitionNo);
            }
        }
    }

    // 실질적인 HDFS 적재를 수행하는 메서드
    private void save(int partitionNo) {
        if (bufferString.get(partitionNo).size() > 0)
            try {
                //HDFS 에 저장할 파일 이름. 파티변 번호, 오프셋 번호를 파일 이름을 보고 알 수 있도록 정함
                String fileName = "/data/color-" + partitionNo + "-" + currentFileOffset.get(partitionNo) + ".log";

                // HDFS 적재를 위한 설정
                Configuration configuration = new Configuration();
                configuration.set("fs.defaultFS", "hdfs://localhost:9000");
                FileSystem hdfsFileSystem = FileSystem.get(configuration);

                // 버퍼 데이터를 파일로 저장하기 위해 FSDataOutputStream 인스턴스를 생성.
                // bufferString 에 쎃인 데이터를 fileOutputStream 에 저장. 저장이 완료된 이후에는 close()메서드를 통해 안전하게 종료
                FSDataOutputStream fileOutputStream = hdfsFileSystem.create(new Path(fileName));
                fileOutputStream.writeBytes(StringUtils.join("\n", bufferString.get(partitionNo)));
                fileOutputStream.close();

                //버퍼 데이터가 적재 완료되었다면 새로 ArrayList 를 선언하여 버퍼 데이터를 초기화
                bufferString.put(partitionNo, new ArrayList<>());
            } catch (Exception e) {
                // HDFS 파일 적재시 Exception 이 발생하면 에러 로그를 남긴다.
                logger.error(e.getMessage(), e);
            }
    }

    // 버퍼에 남아있는  모든 데이터를 저장하기 위한 메서드. 컨슈머 스레드 종료 시에 호출됨
    private void saveRemainBufferToHdfsFile() {
        bufferString.forEach((partitionNo, v) -> this.save(partitionNo));
    }

    // 셧다운 훅이 발생했을 시에 안전한 종료를 위해 consumer 에 wakeup() 메서드를 호출.
    // 남은 버퍼의 데이터를 모두 저장히기 위해 saveRemainBufferToHdfsFile() 메서드도 호출
    public void stopAndWakeup() {
        logger.info("stopAndWakeup");
        consumer.wakeup();
        saveRemainBufferToHdfsFile();
    }
}