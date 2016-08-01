package io.github.homepy.meow.storm.wordcount;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2107029392155190729L;
	// 用来向其他Spout发射tuple的发射器
	private OutputCollector collector;

	/*
	 * bolt初始化时调用prepare(), 类似于Spout的open()
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	/*
	 * 每次从订阅的数据流中接受一个tuple, 调用该方法
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple input) {
		/*接收从SentenceSpout的发射器发射过来的tuple,因为SentenceSpout中声明的tuple字段为sentence,故getStringByField方法的参数为sentence
		 */
		String sentence = input.getStringByField("sentence");// 
		String[] words = sentence.split(" ");// 将字符串分解成一个个的单词
		for (String word : words) {
			this.collector.emit(input, new Values(word));// 锚定 将每个单词构造成tuple并向后发射
		}
		this.collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));// 定义SplitSentenceBolt发送的tuple的字段key为word
	}
}