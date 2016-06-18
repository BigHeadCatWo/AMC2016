package com.alibaba.middleware.race.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


/**
 * ����һ���ܼ򵥵�����
 * ѡ�ֵ������ύ����Ⱥ���������г�ʱ���õġ�ÿ��ѡ�ֵ����������20���ӣ�һ���������ʱ��
 * ���ǻὫѡ������ɱ����
 */

/**
 * ѡ����������࣬���Ƕ��������com.alibaba.middleware.race.jstorm.RaceTopology
 * ��Ϊ���Ǻ�̨��ѡ�ֵ�git�������ش�����������е������Ĭ����com.alibaba.middleware.race.jstorm.RaceTopology��
 * �����������·��һ��Ҫ��ȷ
 */
public class RaceTopology {

    @SuppressWarnings("unused")
	private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);
    public static int Interval=6000;

    public static void main(String[] args) throws Exception {       	    	
    	//topology�����Զ�������þ��������conf
        Config conf = new Config();
        int classify_Parallelism_hint = 1;
        int statistic_Parallelism_hint = 1;
        int tair_Parallelism_hint = 1;
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("classify", new MessageClassifySpout(), classify_Parallelism_hint);
      //��ʾ����spout�����ݣ�������shuffle��ʽ����ÿ��spout�����ѯ����tuple����һ��bolt��
        builder.setBolt("taobao", new TaobaoStatistic(), statistic_Parallelism_hint).shuffleGrouping("classify", "Taobao_Stream_Id");
        builder.setBolt("tmall", new TmallStatistic(), statistic_Parallelism_hint).shuffleGrouping("classify", "Tmall_Stream_Id");
        builder.setBolt("taobaoTair", new TaobaoTair(), tair_Parallelism_hint).shuffleGrouping("taobao");
        builder.setBolt("tmallTair", new TmallTair(), tair_Parallelism_hint).shuffleGrouping("tmall");
        builder.setBolt("PcOrWireless", new PcOrWirelessStatistic(), statistic_Parallelism_hint)
 	   		   .shuffleGrouping("tmall", "PcOrWireless_Stream_Id")
 	           .shuffleGrouping("taobao", "PcOrWireless_Stream_Id");
        builder.setBolt("PcOrWirelessTair", new PcOrWirelessTair(), tair_Parallelism_hint).shuffleGrouping("PcOrWireless");
        
        
        String topologyName = RaceConfig.JstormTopologyName;

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());
        Thread.sleep(1000);

        Thread.sleep(20*60*1000);

        cluster.killTopology(topologyName);
        cluster.shutdown();
//        try {
//            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
    }
}