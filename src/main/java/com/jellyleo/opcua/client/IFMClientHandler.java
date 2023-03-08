/**
 * Created by Jellyleo on 2019年12月12日
 * Copyright © 2019 jellyleo.com 
 * All rights reserved. 
 */
package com.jellyleo.opcua.client;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import com.jellyleo.opcua.entity.NodeEntity;
import com.jellyleo.opcua.entity.VSEEntity;
import com.jellyleo.opcua.util.DataUtils;
import com.jellyleo.opcua.util.KafkaUtils;
import com.jellyleo.opcua.util.RedisOperator;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.nodes.Node;
import org.eclipse.milo.opcua.sdk.client.api.nodes.VariableNode;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @ClassName: ClientHandler
 * @Description: 客户端处理
 * @author Jellyleo
 * @date 2019年12月12日
 */
@Slf4j
@Service
public class IFMClientHandler {

	// 客户端实例
	private OpcUaClient client = null;

	@Autowired
	private ClientRunner clientRunner;

	@Autowired
	RedisOperator redisOperator;

	//测试时用map代替redis
	private Map<String, VSEEntity> map = new HashMap<>();

	@Value("${kafka.broker}")
	private String broker;

	@Value("${kafka.topic}")
	private String topic;

	@Value("${opc.nodeId.namespaceIndex:3}")
	private int namespaceIndex;

	@Value("${ifm.objects:''}")
	private String ifmObjectsStr;


	public void connect() throws Exception {
		client = clientRunner.run();
		// 创建连接
		client.connect().get();
	}

	public void disconnect() throws Exception {
		if (client == null) {
			return;
		}

		// 断开连接
		clientRunner.getFuture().complete(client);
		client = null;
	}

	public void start() throws Exception {
		//连接opc server
		connect();

		List<String> identifiers = new ArrayList<>();

		if ("".equals(ifmObjectsStr)) {
			log.info("no object to read and subscribe");
			return;
		}

		String[] split = ifmObjectsStr.split(";");
		for(String s : split) {
			String[] object = s.split("-");
			String address = object[0];
			int max = Integer.parseInt(object[1]);
			String temp = "ifm.VSE." + address + ".Objects.Object";
//			log.info("identifier: {}", temp);
			for (int i = 1; i <= max; i++) {
				if (i < 10) {
					identifiers.add(temp + "0" + i);
				} else {
					identifiers.add(temp + i);
				}
			}
		}

		List<NodeId> nodeIds = new ArrayList<>();
		for (String identifier : identifiers) {
			NodeId nodeId = new NodeId(namespaceIndex, identifier);
			// 不是订阅nodeId本身，而是其下的3个子节点
			List<NodeId> ids = browseAndRead(nodeId);
			// 目前只需要value值
			nodeIds.add(ids.get(3)); // Value
//			nodeIds.add(ids.get(4)); // Maximum
//			nodeIds.add(ids.get(8)); // RotSpeed
		}

		subscribe(nodeIds);
	}

	/**
	 * @MethodName: subscribe
	 * @Description: 订阅节点变量
	 * @throws Exception
	 * @CreateTime 2019年12月18日 上午10:38:11
	 */
	public void subscribe(List<NodeId> nodes) throws Exception {

		// 查询订阅对象，没有则创建
		UaSubscription subscription = null;
		ImmutableList<UaSubscription> subscriptionList = client.getSubscriptionManager().getSubscriptions();
		if (CollectionUtils.isEmpty(subscriptionList)) {
			subscription = client.getSubscriptionManager().createSubscription(1000.0).get();
		} else {
			subscription = subscriptionList.get(0);
		}

		// 监控项请求列表
		List<MonitoredItemCreateRequest> requests = new ArrayList<>();

		if (!CollectionUtils.isEmpty(nodes)) {
			for (NodeId nodeId : nodes) {
				// 创建监控的参数
				MonitoringParameters parameters = new MonitoringParameters(subscription.nextClientHandle(), 1000.0, // sampling
						// interval
						null, // filter, null means use default
						Unsigned.uint(10), // queue size
						true // discard oldest
				);

				// 创建订阅的变量， 创建监控项请求
				MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
						new ReadValueId(nodeId, AttributeId.Value.uid(),
								null, null),
						MonitoringMode.Reporting, parameters);
				requests.add(request);
			}
		}

		// 创建监控项，并且注册变量值改变时候的回调函数
		subscription.createMonitoredItems(TimestampsToReturn.Both, requests, (item, id) -> {
			item.setValueConsumer(this::onSubscriptionValue);
		}).get();

	}

	private void onSubscriptionValue(UaMonitoredItem item, DataValue dataValue) {
		NodeId nodeId = item.getReadValueId().getNodeId();
//		log.info("node={}, value={}", nodeId, dataValue.getValue().getValue());
		String identifier = nodeId.getIdentifier().toString();
		String[] split = identifier.split("\\.");
		String name = split[split.length - 1];
		identifier = identifier.substring(0, identifier.length() - name.length() - 1);

//		String redisValue = redisOperator.get(identifier);
//		if (redisValue == null) {
//			log.info("no data in redis");
//			return;
//		}
//		VSEEntity vseEntity = JSON.parseObject(redisValue, VSEEntity.class);

		VSEEntity vseEntity = map.get(identifier);
		if (vseEntity == null) {
			log.info("no such data in map");
			return;
		}

		vseEntity.setTs(Objects.requireNonNull(dataValue.getServerTime()).getJavaTime());
		Double value;
		if (dataValue.getValue().getValue() != null) {
			value = ((Float) dataValue.getValue().getValue()).doubleValue();
		} else {
			value = null;
		}
		vseEntity.setValue(value);

		log.info("nodeId: {}", vseEntity.getNodeId());

		// 数据格式转换
		String jsonStr = DataUtils.dataConversion(vseEntity);
		if (!StringUtils.isEmpty(jsonStr)) {
			// 发送到kafka
			KafkaUtils.send(broker, topic, jsonStr);
		}

//		long current = Objects.requireNonNull(dataValue.getServerTime()).getJavaTime();
//		//上次更新时间距本次更新时间大于1s时认为一个完整更新已结束，发送到kafka
//		if (current - vseEntity.getTs() > 1000) {
////			System.out.println(vseEntity);
//			log.info("nodeId: {}", vseEntity.getNodeId());
//
//			// 数据格式转换
//			String jsonStr = DataUtils.dataConversion(vseEntity);
//			if (!StringUtils.isEmpty(jsonStr)) {
//				// 发送到kafka
//				KafkaUtils.send(broker, topic, jsonStr);
//			}
//		}
//		Double value;
//		if (dataValue.getValue().getValue() != null) {
//			value = ((Float) dataValue.getValue().getValue()).doubleValue();
//		} else {
//			value = null;
//		}
//
//		vseEntity.setTs(current);
//		if (name.equalsIgnoreCase("value")) {
//			vseEntity.setValue(value);
//		} else if (name.equalsIgnoreCase("maximum")) {
//			vseEntity.setMaximum(value);
//		} else if (name.equalsIgnoreCase("warning")) {
//			vseEntity.setWarning(value);
//		} else if (name.equalsIgnoreCase("damage")) {
//			vseEntity.setDamage(value);
//		} else if (name.equalsIgnoreCase("rotSpeed")) {
//			vseEntity.setRotSpeed(value);
//		}

//		redisOperator.set(identifier, JSON.toJSONString(vseEntity));
//		map.put(identifier, vseEntity);
	}

	public boolean isNumeric(String str){
		for (int i = str.length();--i>=0;){
			if (!Character.isDigit(str.charAt(i))){
				return false;
			}
		}
		return true;
	}

	public List<NodeId> browseAndRead(NodeId nodeId) throws Exception {
		List<? extends Node> nodes = client.getAddressSpace().browse(nodeId).get();

		List<NodeId> nodeIds = nodes.stream().map(node -> {
			try {
				return node.getNodeId().get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}).collect(Collectors.toList());
		List<DataValue> dataValues = client.readValues(0.0, TimestampsToReturn.Both, nodeIds).get();

		for (int i = 0; i < dataValues.size(); i++) {
			System.out.println("node: " + nodeIds.get(i).getIdentifier() +
					", value: " + dataValues.get(i).getValue().getValue());

		}
		DateTime serverTime = dataValues.get(0).getServerTime();
		DateTime sourceTime = dataValues.get(0).getSourceTime();
		System.out.println("serverTime: " + serverTime);
		System.out.println("sourceTime: " + sourceTime);
		System.out.println("timestamp: " + Objects.requireNonNull(serverTime).getJavaTime());

		VSEEntity vseEntity = new VSEEntity();
		vseEntity.setName((String) dataValues.get(0).getValue().getValue());
		vseEntity.setType((Integer) dataValues.get(1).getValue().getValue());
		vseEntity.setId((Integer) dataValues.get(2).getValue().getValue());
		Float value = (Float) dataValues.get(3).getValue().getValue();
		if (value == null) {
			vseEntity.setValue(null);
		} else {
			vseEntity.setValue(value.doubleValue());
		}
		Float maximum = (Float) dataValues.get(4).getValue().getValue();
		if (maximum == null) {
			vseEntity.setMaximum(null);
		} else {
			vseEntity.setMaximum(maximum.doubleValue());
		}
		vseEntity.setUnit((String) dataValues.get(5).getValue().getValue());
		vseEntity.setState((Integer) dataValues.get(6).getValue().getValue());
		vseEntity.setError((String) dataValues.get(7).getValue().getValue());
		Float rotSpeed = (Float) dataValues.get(8).getValue().getValue();
		if (rotSpeed == null) {
			vseEntity.setRotSpeed(null);
		} else {
			vseEntity.setRotSpeed(rotSpeed.doubleValue());
		}
		//9 refValue
		Float warning = (Float) dataValues.get(10).getValue().getValue();
		if (warning == null) {
			vseEntity.setWarning(null);
		} else {
			vseEntity.setWarning(warning.doubleValue());
		}
		Float damage = (Float) dataValues.get(11).getValue().getValue();
		if (damage == null) {
			vseEntity.setDamage(null);
		} else {
			vseEntity.setDamage(damage.doubleValue());
		}
		//12 baseline
		vseEntity.setRotValueId((Integer) dataValues.get(13).getValue().getValue());
		//14 refValueID
		//15 inputID
		//16 inputType

		// ifm.VSE.192.168.0.33:3321.Objects.Object04
		String[] split = nodeId.getIdentifier().toString().split(":");
		vseEntity.setIp(split[0].substring(8));
		vseEntity.setPort(Integer.parseInt(split[1].split("\\.")[0]));
		vseEntity.setNodeId(nodeId.getIdentifier().toString());
		vseEntity.setTs(Objects.requireNonNull(serverTime).getJavaTime());

		map.put(nodeId.getIdentifier().toString(), vseEntity);
//		redisOperator.set(nodeId.getIdentifier().toString(), JSON.toJSONString(vseEntity));

		// 发送到kafka
		System.out.println(vseEntity);
//		KafkaUtils.send(broker, topic, JSON.toJSONString(vseEntity));

		return nodeIds;
	}

}
