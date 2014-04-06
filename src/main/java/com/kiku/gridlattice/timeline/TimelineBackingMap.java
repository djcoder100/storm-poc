package com.kiku.gridlattice.timeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;
import com.kiku.gridlattice.StormDB;
import com.kiku.gridlattice.Pair;
import backtype.storm.task.IMetricsContext;

public class TimelineBackingMap implements IBackingMap<HourlyTimeline> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimelineBackingMap.class);

	public static StateFactory FACTORY = new StateFactory() {
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			// TODO: replace this with OpaqueMap (but our spout never replays anything anyway... ^__^)
			return NonTransactionalMap.build(new TimelineBackingMap());
		}
	};

	@Override
	public List<HourlyTimeline> multiGet(List<List<Object>> keys) {
        LOGGER.info(String.format("multiGet=%s", keys.size(), keys));
		return StormDB.DB.getTimelines(toTimelineQueryKeys(keys));
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<HourlyTimeline> timelines) {
        LOGGER.info(String.format("multiPut=%s", keys.size(), keys));
		StormDB.DB.upsertTimelines(timelines);
	}

	private List<Pair<String, Long>> toTimelineQueryKeys(List<List<Object>> keys) {
		List<Pair<String, Long>> result = new ArrayList<>(keys.size());
		for (List<Object> key: keys) {
			// the keys we get here are those specified in the groupBy => Roomid and startId
			result.add(new Pair<String, Long>((String) key.get(0), (Long) key.get(1)));
		}
		return result;
	}

}
