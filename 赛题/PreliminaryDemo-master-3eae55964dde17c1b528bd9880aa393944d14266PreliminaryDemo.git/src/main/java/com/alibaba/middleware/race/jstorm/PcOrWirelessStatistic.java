package com.alibaba.middleware.race.jstorm;

import java.util.*;

import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class PcOrWirelessStatistic implements IRichBolt {
	OutputCollector collector;
	LinkedHashMap<Long, Double> pcCount = new LinkedHashMap<Long, Double>();
	LinkedHashMap<Long, Double> wirelessCount = new LinkedHashMap<Long, Double>();
	double ratio = 0.0;
	boolean pcReadly = false;
	boolean wirelessReadly = false;
	long pcCurrentTime = 0;   // 分别记录PC和wireless两个节点最新具体存储的时间值
    long wirelessCurrentTime = 0;
	
	
	@Override
	public void execute(Tuple tuple) {
		if (tuple.getValue(0).equals("0x00")) {// 收到流结束的标志
			if (pcCount.size() == 0 || wirelessCount.size() == 0) {// 第一次就直接收到流结束标志
				return;
			}else{ // 计算未计算的数据比值然后提交
				if(pcCurrentTime >= wirelessCurrentTime) {
					ratioCount(pcCount, wirelessCount, pcCurrentTime);
					ratio = wirelessCount.get(pcCurrentTime)/pcCount.get(pcCurrentTime);
					collector.emit(new Values(pcCurrentTime, ratio));
					pcCount.remove(pcCurrentTime);
					wirelessCount.remove(pcCurrentTime);
				}else {
					ratioCount(wirelessCount, pcCount, wirelessCurrentTime);
					ratio = wirelessCount.get(wirelessCurrentTime)/pcCount.get(wirelessCurrentTime);
					collector.emit(new Values(wirelessCurrentTime, ratio));
					pcCount.remove(wirelessCurrentTime);
					wirelessCount.remove(wirelessCurrentTime);
				}				
			}
		}else {
			PaymentMessage payment = (PaymentMessage) tuple.getValue(0);
			long createTime = (payment.getCreateTime() / 1000 / 60) * 60;
			short paySource = payment.getPaySource();    //获取支付平台信息0/1		
			
			if(paySource == 0) {
				pcCurrentTime = createTime;
				if (!pcCount.containsKey(createTime)) {
					pcCount.put(createTime, payment.getPayAmount());
					wirelessCount.put(createTime, 0.0);  //无论是哪种平台，只要进入下分钟就存储一下。防止空指针（这一分钟无数据）
				} else {
					pcCount.put(createTime, pcCount.get(createTime) + payment.getPayAmount());
				}
				if (pcCount.size() > 1) { 
					pcReadly = true;               //标志位，表示此map种存在没有计算ratio的time值 
					if (wirelessReadly = true) {   //Pc和无线都有需要计算的time值，则计入计算ratio
						if(pcCount.size() >= wirelessCount.size()) {
							ratioCount(wirelessCount, pcCount, wirelessCurrentTime);
						}else {
							ratioCount(pcCount, wirelessCount, pcCurrentTime);
						}					
					}
			     }		
			}else if(paySource == 1) {
				wirelessCurrentTime = createTime;
				if (!wirelessCount.containsKey(createTime)) {
					wirelessCount.put(createTime, payment.getPayAmount());
					pcCount.put(createTime, 0.0);   //无论是哪种平台，只要进入下分钟就存储一下，防止空指针（这一分钟无数据）
				} else {
					wirelessCount.put(createTime, wirelessCount.get(createTime) + payment.getPayAmount());
				}
				if (wirelessCount.size() > 1) { 
					wirelessReadly  = true;             //标志位，表示此map种存在没有计算ratio的time值 
					if (pcReadly == true) {             //Pc和无线都有需要计算的time值，则计入计算ratio
						if(pcCount.size() >= wirelessCount.size()) {
							ratioCount(wirelessCount, pcCount, wirelessCurrentTime);
						}else {
							ratioCount(pcCount, wirelessCount, pcCurrentTime);
						}				
					}				
				}
			}
		}				
    }
	
	public void ratioCount(LinkedHashMap<Long, Double> map1, LinkedHashMap<Long, Double> map2, long time) {  //将time以前的所有数据进行计算比值并提交，然后将数据从map中删除，保留time时刻的数据
		long tmpTime;
		Iterator<Long> iter = map1.keySet().iterator();
		
		while(iter.hasNext()) {
			tmpTime = iter.next();
			if(tmpTime != time) {
				ratio = wirelessCount.get(tmpTime)/pcCount.get(tmpTime);
				collector.emit(new Values(tmpTime, ratio));
				iter.remove();
				//pcCount.remove(tmpTime);
				map2.remove(tmpTime);
			}			
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("minunet", "ratio"));
		declarer.declareStream("PcOrWireless_Stream_Id", new Fields("message"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO 自动生成的方法存根
		return null;
	}

	@Override
	public void cleanup() {
		// TODO 自动生成的方法存根

	}

}
