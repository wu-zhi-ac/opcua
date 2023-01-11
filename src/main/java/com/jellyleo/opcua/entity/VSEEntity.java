package com.jellyleo.opcua.entity;

import lombok.*;

/**
 * @package: com.jellyleo.opcua.entity
 * @Author: lenovo
 * @CreateTime: 2023-01-06 12:22
 * @Description:
 * @Version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class VSEEntity {
    Integer id;
    String name;
    Integer state;
    Integer type;
    String unit;
    Double rotSpeed;
    Integer rotValueId;
    Double value;
    Double damage;
    Double warning;
    Double maximum;
    String error;
    String ip;
    Integer port;
    String nodeId;
    Long ts;
}
