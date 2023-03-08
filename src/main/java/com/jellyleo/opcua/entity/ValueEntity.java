package com.jellyleo.opcua.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

/**
 * @author sunhaitong
 * @date 2021/8/24
 */
@Data
public class ValueEntity {
    public ValueEntity(){}
    public ValueEntity(int kpiId, Double value, int isValid) {
        this.KpiId = kpiId;
        this.Value = value;
        this.IsValid = isValid;
    }

//    @JSONField(name="KpiId")
    private int KpiId;
//    @JSONField(name="Value")
    private Double Value;

    /**
     * 表示是否有效 默认值为1表示有效
     */
//    @JSONField(name="IsValid")
    private int IsValid;

    @JSONField(name="KpiId")
    public int getKpiId() {
        return KpiId;
    }

    @JSONField(name="Value")
    public Double getValue() {
        return Value;
    }

    @JSONField(name="IsValid")
    public int getIsValid() {
        return IsValid;
    }
}
