/**
 * Created by Jellyleo on 2019年12月12日
 * Copyright © 2019 jellyleo.com 
 * All rights reserved. 
 */
package com.jellyleo.opcua.client;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.jellyleo.opcua.entity.VSEEntity;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.nodes.Node;
import org.eclipse.milo.opcua.sdk.client.api.nodes.VariableNode;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.google.common.collect.ImmutableList;
import com.jellyleo.opcua.entity.NodeEntity;

import lombok.extern.slf4j.Slf4j;

import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;

/**
 * @ClassName: ClientHandler
 * @Description: 客户端处理
 * @author Jellyleo
 * @date 2019年12月12日
 */
@Slf4j
@Service
public class ClientHandler {

	// 客户端实例
	private OpcUaClient client = null;

	@Autowired
	private ClientRunner clientRunner;

	private Map<String, VSEEntity> map = new HashMap<>();

	/**
	 * 
	 * @MethodName: connect
	 * @Description: connect
	 * @throws Exception
	 * @CreateTime 2019年12月18日 上午10:41:09
	 */
	public String connect() throws Exception {

		if (client != null) {
			return "客户端已创建";
		}

		client = clientRunner.run();

		if (client == null) {
			return "客户端配置实例化失败";
		}

		// 创建连接
		client.connect().get();
		return "创建连接成功";
	}

	/**
	 * @MethodName: disconnect
	 * @Description: 断开连接
	 * @return
	 * @throws Exception
	 * @CreateTime 2019年12月18日 上午10:45:21
	 */
	public String disconnect() throws Exception {

		if (client == null) {
			return "连接已断开";
		}

		// 断开连接
		clientRunner.getFuture().complete(client);
		client = null;
		return "断开连接成功";
	}

	/**
	 * @MethodName: subscribe
	 * @Description: 订阅节点变量
	 * @throws Exception
	 * @CreateTime 2019年12月18日 上午10:38:11
	 */
	public String subscribe(List<NodeEntity> nodes) throws Exception {

		if (client == null) {
			return "找不到客户端，操作失败";
		}

//		List<Node> ns = client.getAddressSpace().browse(new NodeId(2, "模拟通道一.模拟设备一")).get();

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
			for (NodeEntity node : nodes) {
				// 创建监控的参数
				MonitoringParameters parameters = new MonitoringParameters(subscription.nextClientHandle(), 1000.0, // sampling
						// interval
						null, // filter, null means use default
						Unsigned.uint(10), // queue size
						true // discard oldest
				);
				NodeId nodeId;
				if (isNumeric(node.getIdentifier())) {
					nodeId = new NodeId(node.getIndex(), Integer.parseInt(node.getIdentifier()));
				} else {
					nodeId = new NodeId(node.getIndex(), node.getIdentifier());
				}
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
			item.setValueConsumer((i, v) -> {

				NodeId nodeId = i.getReadValueId().getNodeId();
				log.info("node={}, value={}", nodeId, v.getValue().getValue());
				String identifier = nodeId.getIdentifier().toString();
				String[] split = identifier.split("\\.");
				String name = split[split.length - 1];
				identifier = identifier.substring(0, identifier.length() - name.length() - 1);
//				System.out.println("object: " + identifier);
				if (!map.containsKey(identifier)) {
					System.out.println("no data in map");
					return;
				}
				VSEEntity vseEntity = map.get(identifier);

				long current = Objects.requireNonNull(v.getServerTime()).getJavaTime();
				if (current - vseEntity.getTs() > 1000) {
					// 发送到kafka
					System.out.println(vseEntity);
				}
				Double value;
				if (v.getValue().getValue() != null) {
					value = ((Float) v.getValue().getValue()).doubleValue();
				} else {
					value = null;
				}

				vseEntity.setTs(current);
				if (name.equalsIgnoreCase("value")) {
					vseEntity.setValue(value);
				} else if (name.equalsIgnoreCase("maximum")) {
					vseEntity.setMaximum(value);
				} else if (name.equalsIgnoreCase("warning")) {
					vseEntity.setWarning(value);
				} else if (name.equalsIgnoreCase("damage")) {
					vseEntity.setDamage(value);
				} else if (name.equalsIgnoreCase("rotSpeed")) {
					vseEntity.setRotSpeed(value);
				}
				/*
				ifm.VSE.192.168.0.33:3321.Objects.Object06.Value,ifm.VSE.192.168.0.33:3321.Objects.Object06.Maximum,ifm.VSE.192.168.0.33:3321.Objects.Object06.RotSpeed
				 */

				map.put(identifier, vseEntity);
			});
		}).get();

		return "订阅成功";
	}

	/**
	 * @MethodName: write
	 * @Description: 变节点量写入
	 * @param node
	 * @throws Exception
	 * @CreateTime 2019年12月18日 上午9:51:40
	 */
	public String write(NodeEntity node) throws Exception {

		if (client == null) {
			return "找不到客户端，操作失败";
		}

		NodeId nodeId;
		if (isNumeric(node.getIdentifier())) {
			nodeId = new NodeId(node.getIndex(), Integer.parseInt(node.getIdentifier()));
		} else {
			nodeId = new NodeId(node.getIndex(), node.getIdentifier());
		}

		Variant value = null;
		switch (node.getType()) {
		case "int":
//			value = new Variant(Integer.parseInt(node.getValue().toString()));
			value = new Variant(Unsigned.ushort(Integer.parseInt(node.getValue().toString())));
			break;
		case "boolean":
			value = new Variant(Boolean.parseBoolean(node.getValue().toString()));
			break;
		case "short":
			value = new Variant(Short.parseShort(node.getValue().toString()));
			break;
		case "float":
			value = new Variant(Float.parseFloat(node.getValue().toString()));
			break;
		case "double":
			value = new Variant(Double.parseDouble(node.getValue().toString()));
			break;
		default:
			value = new Variant(node.getValue());
		}
		DataValue dataValue = new DataValue(value, null, null);

		StatusCode statusCode = client.writeValue(nodeId, dataValue).get();

		return "节点【" + node.getIdentifier() + "】写入状态：" + statusCode.isGood();
	}

	/**
	 * @MethodName: read
	 * @Description: 读取
	 * @param node
	 * @return
	 * @throws Exception
	 * @CreateTime 2019年12月19日 下午2:40:34
	 */
	public String read(NodeEntity node) throws Exception {

		if (client == null) {
			return "找不到客户端，操作失败";
		}

		NodeId nodeId;
		if (isNumeric(node.getIdentifier())) {
			nodeId = new NodeId(node.getIndex(), Integer.parseInt(node.getIdentifier()));
		} else {
			nodeId = new NodeId(node.getIndex(), node.getIdentifier());
		}

//		DataValue dataValue = client.readValue(0.0, TimestampsToReturn.Both, nodeId).get();
//		List<DataValue> dataValues = client.readValues(0.0, TimestampsToReturn.Both,
//				Collections.singletonList(nodeId)).get();
//		log.info("{}", dataValue.getValue().getValue());
		VariableNode vnode = client.getAddressSpace().createVariableNode(nodeId);
		DataValue value = vnode.readValue().get();
		log.info("Value={}", value);

		Variant variant = value.getValue();
		log.info("Variant={}", variant.getValue());

//		log.info("BackingClass={}", BuiltinDataType.getBackingClass(variant.getDataType().get()));

		return "节点【" + node.getIdentifier() + "】：" + variant.getValue();
	}

	// 排除Server Aliases节点
	private static final NodeId[] whiteList = { Identifiers.Server, new NodeId(0, 23470) };

	public void browse(NodeId nodeId, int cnt) throws Exception {
		List<? extends Node> nodes;

		if (nodeId == null) {
//			nodes = client.getAddressSpace().browseNode(Identifiers.ObjectsFolder);
			nodes = client.getAddressSpace().browse(Identifiers.ObjectsFolder).get();
		} else {
			nodes = client.getAddressSpace().browse(nodeId).get();
		}


		for (Node nd : nodes) {
			boolean flag = true;
			NodeId id = nd.getNodeId().get();
			for (NodeId white : whiteList) {
				if (white.getIdentifier().equals(id.getIdentifier())) {
					flag = false;
					break;
				}
			}
			if (!flag) {
				continue;
			}

			String browseName = nd.getBrowseName().get().getName();
			//排除系统性节点，这些系统性节点名称一般都是以"_"开头
			if (Objects.requireNonNull(browseName).startsWith("_")) {
				continue;
			}
			for (int i = 0; i < cnt; i++) {
				System.out.print("  ");
			}
			System.out.println(browseName + "   " + id.getIdentifier() + "   " + id.getNamespaceIndex());
			browse(id, cnt + 1);
		}
	}

	public boolean isNumeric(String str){
		for (int i = str.length();--i>=0;){
			if (!Character.isDigit(str.charAt(i))){
				return false;
			}
		}
		return true;
	}

	public String reads(List<NodeEntity> nodes) throws ExecutionException, InterruptedException {

		if (client == null) {
			return "找不到客户端，操作失败";
		}

		List<NodeId> nodeIds = new ArrayList<>();
		for(NodeEntity node : nodes) {
			if (isNumeric(node.getIdentifier())) {
				nodeIds.add(new NodeId(node.getIndex(), Integer.parseInt(node.getIdentifier())));
			} else {
				nodeIds.add(new NodeId(node.getIndex(), node.getIdentifier()));
			}
		}
		List<DataValue> dataValues = client.readValues(0.0, TimestampsToReturn.Both, nodeIds).get();

		StringBuilder stringBuilder = new StringBuilder();
		for (int i = 0; i < dataValues.size(); i++) {
//			stringBuilder.append("node: " + nodes.get(i).getIdentifier() +
//					", value: " + dataValues.get(i).getValue().getValue() + "\n");
			System.out.println("node: " + nodes.get(i).getIdentifier() +
					", value: " + dataValues.get(i).getValue().getValue());

//			log.info("node: {}, value: {}",
//					nodes.get(i).getIdentifier(), dataValues.get(i).getValue().getValue());
		}

		return "success";
	}

	public void browseForRead(NodeId nodeId) throws Exception {
		List<? extends Node> nodes;

		if (nodeId == null) {
			nodes = client.getAddressSpace().browse(Identifiers.ObjectsFolder).get();
		} else {
			nodes = client.getAddressSpace().browse(nodeId).get();
		}

		for (Node nd : nodes) {
			NodeId id = nd.getNodeId().get();
			String browseName = nd.getBrowseName().get().getName();
			//排除系统性节点，这些系统性节点名称一般都是以"_"开头
			if (Objects.requireNonNull(browseName).contains("_")) {
				continue;
			}
//			System.out.println(browseName + " " + id.getIdentifier() + " " + id.getNamespaceIndex());
			System.out.print(id.getIdentifier() + ",");
		}
		System.out.println();
	}

	public void browseAndRead(NodeId nodeId) throws Exception {
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
		// 发送到kafka
		System.out.println(vseEntity);
	}

}
