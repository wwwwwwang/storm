package com.madhouse.dsp;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by Madhouse on 2017/9/25.
 */
public class WordCountTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        //1、准备一个TopologyBuilder
        //storm框架支持多语言，在Java环境下创建一个拓扑，需要使用TopologyBuilder
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        //MySpout类，在已知的英文句子中，所及发送一条句子出去
        topologyBuilder.setSpout("mySpout", new SpoutByWhsh(), 2);
        //MySplitBolt类，主要是将一行一行的文本内容切割成单词
        topologyBuilder.setBolt("mybolt1", new SplitBoltByWhsh(), 2).shuffleGrouping("mySpout");
        //MyCountBolt类，负责对单词的频率进行累加
        topologyBuilder.setBolt("mybolt2", new CountBoltByWhsh(), 4).fieldsGrouping("mybolt1", new Fields("word"));
        /**
         * i
         * am
         * lilei
         * love
         * hanmeimei
         */
        //2、创建一个configuration，用来指定当前topology 需要的worker的数量
        //启动topology的配置信息
        Config conf = new Config();
        //定义你希望集群分配多少个工作进程给你来执行这个topology
        //config.setNumWorkers(2);

        //3、提交任务  -----两种模式 本地模式和集群模式
        //这里将拓扑名称写死了mywordcount,所以在集群上打包运行的时候，不用写拓扑名称了！也可用arg[0]
        //StormSubmitter.submitTopology("mywordcount", config, topologyBuilder.createTopology());
        //LocalCluster localCluster = new LocalCluster();
        //localCluster.submitTopology("mywordcount",config,topologyBuilder.createTopology());
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, topologyBuilder.createTopology());
            //Thread.sleep(10000);
            Utils.sleep(10000);
            cluster.killTopology("word-count");
            cluster.shutdown();

            //cluster.submitTopology("firstTopo", conf, topology.createTopology());
            //cluster.killTopology("firstTopo");
            //cluster.shutdown();
        }
    }
}
