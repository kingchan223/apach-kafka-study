package com.example;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/*필터링 역할을 하는 클래스*/
public class FilterProcessor implements Processor<String, String> {//스트림 프로세서를 만들기 위해서는 Processor를 implements

    private ProcessorContext context;

    @Override//스트림 프로세서의 생성자
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override//실질적인 프로세싱 로직이 들어가는 부분. 1개의 레코드를 받는 것을 가정하여 데이터를 처리하면 된다.
    public void process(String key, String value) {//메시지 키, 값을 파라미터로 받음
        //메시지 값의 길이가 5 이상인 경우를 필터링.
        //필터링된 데이터의 경우 forward()메서드를 사용하여 다음 토폴로지(다음 프로세서)로 넘어가도록 한다.
        if(value.length() >= 5) context.forward(key, value);
        context.commit();//처리가 완료된 후에는 commit()을 명시적으로 호출하여 데이터가 처리되었음을 선언.
    }

    @Override//FilterProcessor가 종료되기 이전에 호출되는 메서드
    public void close() {
        //프로세싱을 하기 위해 사용했던 리소스를 해제하는 구문을 넣는다.
        //여기서는 해제할 리소스가 없으므로 생략
    }
}
