package com.jellyleo.opcua.util;

import com.alibaba.fastjson.JSON;
import com.jellyleo.opcua.entity.VSEEntity;

/**
 * @package: com.jellyleo.opcua.util
 * @Author: lenovo
 * @CreateTime: 2023-01-11 11:57
 * @Description:
 * @Version: 1.0
 */
public class KafkaTest {
    public static void main(String[] args) {
        VSEEntity vseEntity = new VSEEntity(1, "名字", 1, 1,
                "m/s", 1.1, 1, 2.3,
                5.5, 4.2, 1.1, "warn",
                "127.0.0.1", 3321, "nodeId--", 1L);
        String s = JSON.toJSONString(vseEntity);
        KafkaUtils.send("127.0.0.1:9092", "test", s);
    }
}
