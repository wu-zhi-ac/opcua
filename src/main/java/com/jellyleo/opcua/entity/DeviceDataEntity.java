package com.jellyleo.opcua.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * @author sunhaitong
 * @date 2021/8/24
 */
@Data
public class DeviceDataEntity {
    /**
     * traceId
     */
    private String traceId;

    /**
     * 设备Id
     */
//    @JSONField(name="DevId")
    private String DevId;

    /**
     * 测点Id
     */
//    @JSONField(name="PointId")
    private String PointId;

    /**
     * 时间
     */
//    @JSONField(name="DTime")
    private String DTime;

    /**
     * 特征值数据
     */
//    @JSONField(name="Values")
    private List<ValueEntity> Values;

    public String getTraceId() {
        return traceId;
    }

    @JSONField(name="DevId")
    public String getDevId() {
        return DevId;
    }

    @JSONField(name="PointId")
    public String getPointId() {
        return PointId;
    }

    @JSONField(name="DTime")
    public String getDTime() {
        return DTime;
    }

    @JSONField(name="Values")
    public List<ValueEntity> getValues() {
        return Values;
    }
}
