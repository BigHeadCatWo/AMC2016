package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.impl.DefaultTairManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class PcOrWirelessTair implements IRichBolt {

	DefaultTairManager tairManager;

	@Override
	public void execute(Tuple input) {
		long minuteTime = input.getLong(0);
		double ratio = input.getDouble(1);

		String key = RaceConfig.prex_ratio + minuteTime;
		Result<DataEntry> result = tairManager.get(RaceConfig.TairNamespace, key);
		if (result.isSuccess()) {
			if (result.getValue() != null) {// ���ݴ���
				tairManager.put(RaceConfig.TairNamespace, key, ratio);
				System.out.println("PcOrWireless��ֵ��ȡ: " + key + "  " + ratio);
			} else {// ���ݲ�����
				System.out.println("PcOrWireless��ֵ�޴�ʱ�̵����ݣ�����");
			}
		}else {
			System.out.println("������޴����ݣ�������");
		}

		/*
		 * System.out.println("******************************"+minuteTime);
		 * 
		 * ResultCode resultCode = tairManager.put(RaceConfig.TairNamespace,
		 * RaceConfig.prex_ratio + minuteTime, ratio);
		 * if(resultCode.isSuccess()) { System.out.println(
		 * "ratio insert success"); }
		 */
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		tairManager = new DefaultTairManager();
		ArrayList<String> confServers = new ArrayList<String>();
		confServers.add(RaceConfig.TairConfigServer);
		confServers.add(RaceConfig.TairSalveConfigServer);
		tairManager.setConfigServerList(confServers);
		tairManager.setGroupName(RaceConfig.TairGroup);
		tairManager.init();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO �Զ����ɵķ������

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO �Զ����ɵķ������
		return null;
	}

	@Override
	public void cleanup() {
		// TODO �Զ����ɵķ������

	}

}
