package com.jellyleo.opcua;

import com.alibaba.fastjson.JSON;
import com.jellyleo.opcua.entity.VSEEntity;
import com.jellyleo.opcua.util.KafkaUtils;
import com.jellyleo.opcua.util.RedisOperator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @package: java.com.jellyleo.opcua
 * @Author: lenovo
 * @CreateTime: 2023-01-15 18:00
 * @Description:
 * @Version: 1.0
 */
@SpringBootTest
class KafkaTest {
    @Autowired
    RedisOperator redisOperator;

    @Test
    public void test() {
//        redisOperator.set("hello", "world");
        redisOperator.set("hello", "world!!!  ha");
    }

    public void sendToKafka() {
        VSEEntity vseEntity = new VSEEntity(1, "名字", 1, 1,
                "m/s", 1.1, 1, 2.3,
                5.5, 4.2, 1.1, "warn",
                "127.0.0.1", 3321, "nodeId--", 1L);
        String s = JSON.toJSONString(vseEntity);
        KafkaUtils.send("127.0.0.1:9092", "test", s);
//        KafkaTest kafkaTest = new KafkaTest();
//        kafkaTest.redisOperator.set("hello", "world");

    }
}
