package com.alibaba.middleware.race;

import java.io.Serializable;

@SuppressWarnings("serial")
public class RaceConfig implements Serializable {

    //��Щ��дtair key��ǰ׺
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";


    //��Щjstorm/rocketMq/tair �ļ�Ⱥ������Ϣ����Щ������Ϣ����ʽ�ύ����ǰ�ᱻ����
    public static String JstormTopologyName = "test";
    public static String MetaConsumerGroup = "xxx";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String TairConfigServer = "192.168.1.235:51980";
    public static String TairSalveConfigServer = "192.168.1.150:51980";
    public static String TairGroup = "group_1";
    public static Integer TairNamespace = 1;
}
