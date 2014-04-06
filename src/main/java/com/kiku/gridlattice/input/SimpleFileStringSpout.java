package com.kiku.gridlattice.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Quick and dirty spout which emits one tuple containing one single String value for each line found in a file. <br />
 * There is no support of fail() nor ack() here => failed tuples are just lost
 * 
 */
public class SimpleFileStringSpout extends BaseRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleFileStringSpout.class);
	private final String emittedTupleName;
    private int linesRead = 0;
    private String fileName;

	private static BufferedReader br;
	private SpoutOutputCollector collector;

	public SimpleFileStringSpout(String sourceFileName, String emittedTupleName) {
		super();
        fileName = sourceFileName;
		this.emittedTupleName = emittedTupleName;
		try {
            LOGGER.info(String.format("Start reading file=%s", sourceFileName));
			br = new BufferedReader(new FileReader(new File(sourceFileName)));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void close() {
		if (br != null) {
			try {
				br.close();
				br = null;
			} catch (IOException e) {
				System.err.println("could not close input stream to json file containing events.");
				e.printStackTrace();
			}
		}
	}

	public void nextTuple() {
		try {
			String rawEvent = br.readLine();

			if (rawEvent != null) {

				try {
					String messageId = UUID.randomUUID().toString();
                    linesRead++;
                    LOGGER.info(String.format("%s Reading line id=%s count=%s ..", fileName,messageId,linesRead));
					collector.emit(new Values(rawEvent), messageId);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				Utils.sleep(1);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(emittedTupleName));
	}

	@Override
	public void ack(Object msgId) {

	}

	@Override
	public void fail(Object msgId) {
		System.err.println("oups: failed message: " + msgId + ", not retrying");
	}

}
