package io.github.homepy.meow.storm.wordcount;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5269603842279771966L;
	private OutputCollector collector;
	// 通常应避免将信息存在Bolt中.Bolt执行异常或重新指派时，数据会丢失.应持久化.
	private Map<String, Long> counts = null;// 统计每个单词出现的次数

	/*
	 * topology发布时,先序列化组件,然后通过网络发送到集群中。
	 * 故实例变量应可序列化。
	 * 一般在构造函数中对基本数据类型和可序列化的Object进行赋值和实例化,
	 * 在prepare()中对不可序列化的对象进行实例化
	 */
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String, Long>();// 初始化实例变量
	}

	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = this.counts.get(word);
		if (count == null) {
			count = 0L;
		}
		count++;
		this.counts.put(word, count);// 更新该单词在HashMap中的统计次数
		this.collector.emit(input, new Values(word, count)); // List values
		this.collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));  // List fields
	}
}