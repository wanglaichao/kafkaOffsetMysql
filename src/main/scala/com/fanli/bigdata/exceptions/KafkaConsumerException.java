package com.fanli.bigdata.exceptions;

/**
 * Created by laichao.wang on 2018/11/30.
 * @author laichao.wang
 */
public class KafkaConsumerException extends RuntimeException{


    public KafkaConsumerException(String message) {
        super(message);
    }

    public KafkaConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaConsumerException(Throwable cause) {
        super(cause);
    }

}
