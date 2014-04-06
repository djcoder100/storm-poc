package com.kiku.gridlattice;

import com.kiku.gridlattice.period.PresencePeriodBackingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentTopology;
import storm.trident.spout.RichSpoutBatchExecutor;
import com.kiku.gridlattice.input.EventBuilder;
import com.kiku.gridlattice.input.ExtractCorrelationId;
import com.kiku.gridlattice.input.SimpleFileStringSpout;
import com.kiku.gridlattice.period.PeriodBuilder;
import com.kiku.gridlattice.timeline.BuildHourlyUpdateInfo;
import com.kiku.gridlattice.timeline.IsPeriodComplete;
import com.kiku.gridlattice.timeline.TimelineBackingMap;
import com.kiku.gridlattice.timeline.TimelineUpdater;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


/**
 * definition + execution of the Trident topology
 *
 */
public class RunMe {

    private static final Logger LOGGER = LoggerFactory.getLogger(RunMe.class);
    public static void main(String[] args)  {
		
		// wipes out DB content at every start-up
        LOGGER.info("Start storm-poc ....");
		StormDB.DB.reset();
		
		TridentTopology topology = new TridentTopology();

		topology
			// reading events
			.newStream("occupancy", new SimpleFileStringSpout("data/events.json", "rawOccupancyEvent"))
			.each(new Fields("rawOccupancyEvent"), new EventBuilder(), new Fields("occupancyEvent"))
			
			// gathering "enter" and "leave" events into "presence periods"
			.each(new Fields("occupancyEvent"), new ExtractCorrelationId(), new Fields("correlationId"))
			.groupBy(new Fields("correlationId"))
			.persistentAggregate( PresencePeriodBackingMap.FACTORY, new Fields("occupancyEvent"), new PeriodBuilder(), new Fields("presencePeriod"))
			.newValuesStream()
			
			// building room timeline 
			.each(new Fields("presencePeriod"), new IsPeriodComplete())
			.each(new Fields("presencePeriod"), new BuildHourlyUpdateInfo(), new Fields("roomId", "roundStartTime"))
			.groupBy(new Fields("roomId", "roundStartTime"))
			.persistentAggregate( TimelineBackingMap.FACTORY, new Fields("presencePeriod","roomId", "roundStartTime"), new TimelineUpdater(), new Fields("hourlyTimeline"))
			;
		
		
		Config config = new Config();
		config.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 100);
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("occupancyTopology", config, topology.build());

		// this is soooo elegant...
        LOGGER.info("Waiting 1min...");
		Utils.sleep(60000);
		cluster.killTopology("occupancyTopology");
		
		StormDB.DB.close();

        LOGGER.info("Completed storm-poc");
	}
	
}
