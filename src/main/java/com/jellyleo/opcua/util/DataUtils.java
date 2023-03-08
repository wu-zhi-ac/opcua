package com.jellyleo.opcua.util;

import com.alibaba.fastjson.JSON;
import com.jellyleo.opcua.entity.DeviceDataEntity;
import com.jellyleo.opcua.entity.VSEEntity;
import com.jellyleo.opcua.entity.ValueEntity;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

/**
 * @package: com.jellyleo.opcua.util
 * @Author: lenovo
 * @CreateTime: 2023-02-28 18:12
 * @Description:
 * @Version: 1.0
 */
public class DataUtils {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static String dataConversion(VSEEntity vseEntity) {
        //暂时没用到 48的Object10，直接丢弃
        if ("ifm.VSE.192.168.0.48:3321.Objects.Object10".equals(vseEntity.getNodeId())) {
            return null;
        }
        DeviceDataEntity deviceData = new DeviceDataEntity();
        ValueEntity valueEntity = new ValueEntity();
        getDevPointAndKpiId(deviceData, valueEntity, vseEntity.getNodeId());

        //格式化时间
        String dTime = sdf.format(new Date(vseEntity.getTs()));
        deviceData.setDTime(dTime);

        deviceData.setTraceId(deviceData.getDevId() + "-"  + deviceData.getPointId() + "-" + vseEntity.getTs());

        //只放单个数据
        valueEntity.setIsValid(1);
        valueEntity.setValue(vseEntity.getValue());
        unionConversion(valueEntity);
        deviceData.setValues(Collections.singletonList(valueEntity));
        return JSON.toJSONString(deviceData);
    }

    //单位转换
    public static void unionConversion(ValueEntity valueEntity) {
        if (valueEntity.getKpiId() == 61001 || valueEntity.getKpiId() == 61003) {
            valueEntity.setValue(valueEntity.getValue() * 1000);
        } else if (valueEntity.getKpiId() == 61002) {
            valueEntity.setValue(valueEntity.getValue() * 100);
        }
    }

    public static void getDevPointAndKpiId(DeviceDataEntity deviceData, ValueEntity valueEntity, String nodeId) {
//        if ("ifm.VSE.192.168.0.46:3321.Objects.Object01".equals(nodeId)
//            || "ifm.VSE.192.168.0.46:3321.Objects.Object02".equals(nodeId)
//            || "ifm.VSE.192.168.0.46:3321.Objects.Object03".equals(nodeId)) {
//            deviceData.setDevId("EQ000060");
//            deviceData.setPointId("01");
//        }

        switch (nodeId) {
            // 192.168.0.46
            case "ifm.VSE.192.168.0.46:3321.Objects.Object01":
                valueEntity.setKpiId(61001);
                deviceData.setDevId("EQ000150");
                deviceData.setPointId("01");
                break;
            case "ifm.VSE.192.168.0.46:3321.Objects.Object02":
                valueEntity.setKpiId(61002);
                deviceData.setDevId("EQ000150");
                deviceData.setPointId("01");
                break;
            case "ifm.VSE.192.168.0.46:3321.Objects.Object03":
                valueEntity.setKpiId(61003);
                deviceData.setDevId("EQ000150");
                deviceData.setPointId("01");
                break;
            case "ifm.VSE.192.168.0.46:3321.Objects.Object04":
                valueEntity.setKpiId(61001);
                deviceData.setDevId("EQ000150");
                deviceData.setPointId("03");
                break;
            case "ifm.VSE.192.168.0.46:3321.Objects.Object05":
                valueEntity.setKpiId(61002);
                deviceData.setDevId("EQ000150");
                deviceData.setPointId("03");
                break;
            case "ifm.VSE.192.168.0.46:3321.Objects.Object06":
                valueEntity.setKpiId(61003);
                deviceData.setDevId("EQ000150");
                deviceData.setPointId("03");
                break;
            case "ifm.VSE.192.168.0.46:3321.Objects.Object07":
                valueEntity.setKpiId(61001);
                deviceData.setDevId("EQ000150");
                deviceData.setPointId("04");
                break;
            case "ifm.VSE.192.168.0.46:3321.Objects.Object08":
                valueEntity.setKpiId(61002);
                deviceData.setDevId("EQ000150");
                deviceData.setPointId("04");
                break;
            case "ifm.VSE.192.168.0.46:3321.Objects.Object09":
                valueEntity.setKpiId(61003);
                deviceData.setDevId("EQ000150");
                deviceData.setPointId("04");
                break;
            case "ifm.VSE.192.168.0.46:3321.Objects.Object10":
                valueEntity.setKpiId(62001);
                deviceData.setDevId("EQ000150");
                deviceData.setPointId("02");
                break;

                //192.168.0.47
            case "ifm.VSE.192.168.0.47:3321.Objects.Object01":
                valueEntity.setKpiId(61001);
                deviceData.setDevId("EQ000152");
                deviceData.setPointId("01");
                break;
            case "ifm.VSE.192.168.0.47:3321.Objects.Object02":
                valueEntity.setKpiId(61002);
                deviceData.setDevId("EQ000152");
                deviceData.setPointId("01");
                break;
            case "ifm.VSE.192.168.0.47:3321.Objects.Object03":
                valueEntity.setKpiId(61003);
                deviceData.setDevId("EQ000152");
                deviceData.setPointId("01");
                break;
            case "ifm.VSE.192.168.0.47:3321.Objects.Object04":
                valueEntity.setKpiId(61001);
                deviceData.setDevId("EQ000152");
                deviceData.setPointId("03");
                break;
            case "ifm.VSE.192.168.0.47:3321.Objects.Object05":
                valueEntity.setKpiId(61002);
                deviceData.setDevId("EQ000152");
                deviceData.setPointId("03");
                break;
            case "ifm.VSE.192.168.0.47:3321.Objects.Object06":
                valueEntity.setKpiId(61003);
                deviceData.setDevId("EQ000152");
                deviceData.setPointId("03");
                break;
            case "ifm.VSE.192.168.0.47:3321.Objects.Object07":
                valueEntity.setKpiId(62001);
                deviceData.setDevId("EQ000152");
                deviceData.setPointId("02");
                break;

            // 192.168.0.48
            case "ifm.VSE.192.168.0.48:3321.Objects.Object01":
                valueEntity.setKpiId(61001);
                deviceData.setDevId("EQ000152");
                deviceData.setPointId("04");
                break;
            case "ifm.VSE.192.168.0.48:3321.Objects.Object02":
                valueEntity.setKpiId(61002);
                deviceData.setDevId("EQ000152");
                deviceData.setPointId("04");
                break;
            case "ifm.VSE.192.168.0.48:3321.Objects.Object03":
                valueEntity.setKpiId(61003);
                deviceData.setDevId("EQ000152");
                deviceData.setPointId("04");
                break;

            case "ifm.VSE.192.168.0.48:3321.Objects.Object04":
                valueEntity.setKpiId(61001);
                deviceData.setDevId("EQ000151");
                deviceData.setPointId("01");
                break;
            case "ifm.VSE.192.168.0.48:3321.Objects.Object05":
                valueEntity.setKpiId(61002);
                deviceData.setDevId("EQ000151");
                deviceData.setPointId("01");
                break;
            case "ifm.VSE.192.168.0.48:3321.Objects.Object06":
                valueEntity.setKpiId(61003);
                deviceData.setDevId("EQ000151");
                deviceData.setPointId("01");
                break;

            case "ifm.VSE.192.168.0.48:3321.Objects.Object07":
                valueEntity.setKpiId(61001);
                deviceData.setDevId("EQ000151");
                deviceData.setPointId("03");
                break;
            case "ifm.VSE.192.168.0.48:3321.Objects.Object08":
                valueEntity.setKpiId(61002);
                deviceData.setDevId("EQ000151");
                deviceData.setPointId("03");
                break;
            case "ifm.VSE.192.168.0.48:3321.Objects.Object09":
                valueEntity.setKpiId(61003);
                deviceData.setDevId("EQ000151");
                deviceData.setPointId("03");
                break;
            case "ifm.VSE.192.168.0.48:3321.Objects.Object10":
                break;
            case "ifm.VSE.192.168.0.48:3321.Objects.Object11":
                valueEntity.setKpiId(62001);
                deviceData.setDevId("EQ000151");
                deviceData.setPointId("02");
                break;
        }

    }
}
