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
	long pcCurrentTime = 0;   // �ֱ��¼PC��wireless�����ڵ����¾���洢��ʱ��ֵ
    long wirelessCurrentTime = 0;
	
	
	@Override
	public void execute(Tuple tuple) {
		if (tuple.getValue(0).equals("0x00")) {// �յ��������ı�־
			if (pcCount.size() == 0 || wirelessCount.size() == 0) {// ��һ�ξ�ֱ���յ���������־
				return;
			}else{ // ����δ��������ݱ�ֵȻ���ύ
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
			short paySource = payment.getPaySource();    //��ȡ֧��ƽ̨��Ϣ0/1		
			
			if(paySource == 0) {
				pcCurrentTime = createTime;
				if (!pcCount.containsKey(createTime)) {
					pcCount.put(createTime, payment.getPayAmount());
					wirelessCount.put(createTime, 0.0);  //����������ƽ̨��ֻҪ�����·��Ӿʹ洢һ�¡���ֹ��ָ�루��һ���������ݣ�
				} else {
					pcCount.put(createTime, pcCount.get(createTime) + payment.getPayAmount());
				}
				if (pcCount.size() > 1) { 
					pcReadly = true;               //��־λ����ʾ��map�ִ���û�м���ratio��timeֵ 
					if (wirelessReadly = true) {   //Pc�����߶�����Ҫ�����timeֵ����������ratio
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
					pcCount.put(createTime, 0.0);   //����������ƽ̨��ֻҪ�����·��Ӿʹ洢һ�£���ֹ��ָ�루��һ���������ݣ�
				} else {
					wirelessCount.put(createTime, wirelessCount.get(createTime) + payment.getPayAmount());
				}
				if (wirelessCount.size() > 1) { 
					wirelessReadly  = true;             //��־λ����ʾ��map�ִ���û�м���ratio��timeֵ 
					if (pcReadly == true) {             //Pc�����߶�����Ҫ�����timeֵ����������ratio
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
	
	public void ratioCount(LinkedHashMap<Long, Double> map1, LinkedHashMap<Long, Double> map2, long time) {  //��time��ǰ���������ݽ��м����ֵ���ύ��Ȼ�����ݴ�map��ɾ��������timeʱ�̵�����
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
		// TODO �Զ����ɵķ������
		return null;
	}

	@Override
	public void cleanup() {
		// TODO �Զ����ɵķ������

	}

}
