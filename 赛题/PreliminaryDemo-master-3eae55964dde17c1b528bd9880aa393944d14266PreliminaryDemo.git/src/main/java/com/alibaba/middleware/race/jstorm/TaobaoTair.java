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
public class TaobaoTair implements IRichBolt {		
	DefaultTairManager tairManager;
	
    

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

	@SuppressWarnings("unused")
	@Override
	public void execute(Tuple input) {
		long minuteTime = input.getLong(0);
		double money = input.getDouble(1);
		String key = RaceConfig.prex_taobao + minuteTime;
		/*ResultCode rc = tairManager.put(RaceConfig.TairNamespace, key, money);
		if(rc.isSuccess()) {
			System.out.println("******�״δ�ȡ: "+ key+"  "+money);
		} else if(ResultCode.VERERROR.equals(rc)) {
			Result<DataEntry> result = tairManager.get(RaceConfig.TairNamespace, key);
			int version = result.getValue().getVersion();
			double amount = (double) result.getValue().getValue();
			tairManager.put(RaceConfig.TairNamespace, key, money+amount, version);
			System.out.println("**�ٴδ�ȡ: "+ key+"  "+(money+amount));
		}*/
		Result<DataEntry> result = tairManager.get(RaceConfig.TairNamespace, key);
		if(result.isSuccess()) {
			if(result.getValue() != null){// ���ݴ���
				double amount = (double) result.getValue().getValue();
				int version = result.getValue().getVersion();
				tairManager.put(RaceConfig.TairNamespace, key, money+amount/*, version*/);
				System.out.println("Taobao�����ٴδ�ȡ: "+ key+"  "+(money+amount));
			}else {//���ݲ�����
				tairManager.put(RaceConfig.TairNamespace, key, money);
				System.out.println("Taobao�����״δ�ȡ: "+ key+"  "+money);
			}
		}
//		ResultCode resultCode = tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_taobao + minuteTime, money);
//		System.out.println(RaceConfig.prex_taobao + minuteTime+": "+money);
//		if(resultCode.isSuccess()) {
//			System.out.println("taobao ����ɹ�");			
//			System.out.println(System.currentTimeMillis()-start);
//		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
