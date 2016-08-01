package io.github.homepy.meow.storm.wordcount;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7673711733061567881L;

	private SpoutOutputCollector collector;// 用来向其他Spout发射tuple
	private String[] sentences = { "my dog has fleas", "i like cold beverages", "the dog ate my homework",
			"don't have a cow man", "i don't think i like fleas" };
	private int index = 0;
	private ConcurrentMap<UUID, Values> pending;

	/*
	 * Spout初始化时调用open() conf提供Storm配置信息 context提供topology中组件的信息
	 * collector提供发射tuple的方法
	 * 
	 * @see backtype.storm.spout.ISpout#open(java.util.Map,
	 * backtype.storm.task.TopologyContext,
	 * backtype.storm.spout.SpoutOutputCollector)
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.pending = new ConcurrentHashMap<UUID, Values>();
	}

	/*
	 * Spout组件调用该方法向输出的collector发射tuple
	 * 
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		Values values = new Values(sentences[index]);
		UUID msgId = UUID.randomUUID();
		this.pending.put(msgId, values);
		this.collector.emit(values, msgId);
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		Utils.sleep(10);
	}

	@Override
	public void ack(Object msgId) {
		this.pending.remove(msgId);
	}

	@Override
	public void fail(Object msgId) {
		this.collector.emit(this.pending.get(msgId), msgId);
	}

	/*
	 * 定义storm组件发射的数据流,数据流的tuple中包含哪些字段
	 * 
	 * @see
	 * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
	 * topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// 标记SentenceSpout发送的tuple的key为sentence
		declarer.declare(new Fields("sentence"));
	}

}