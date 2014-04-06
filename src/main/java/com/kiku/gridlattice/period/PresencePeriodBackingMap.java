package com.kiku.gridlattice.period;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;
import com.kiku.gridlattice.StormDB;
import backtype.storm.task.IMetricsContext;

/**
 * retrieves in Cassandra the list of existing {@link RoomPresencePeriod} at the beginning of a batch and updates them at the end of the
 * batch.
 */
public class PresencePeriodBackingMap implements IBackingMap<RoomPresencePeriod> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PresencePeriodBackingMap.class);

	public static StateFactory FACTORY = new StateFactory() {
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			// our logic is fully idempotent => no Opaque map nor Transactional map required here...
			return NonTransactionalMap.build(new PresencePeriodBackingMap());
		}
	};

	public List<RoomPresencePeriod> multiGet(List<List<Object>> keys) {

        LOGGER.info(String.format("multiGet=%s", keys.size(), keys));
		return StormDB.DB.getPresencePeriods(toCorrelationIdList(keys));
	}

	public void multiPut(List<List<Object>> keys, List<RoomPresencePeriod> newOrUpdatedPeriods) {
        LOGGER.info(String.format("multiPut=%s", newOrUpdatedPeriods.size(), keys));
		StormDB.DB.upsertPresencePeriods(newOrUpdatedPeriods);
	}

	private List<String> toCorrelationIdList(List<List<Object>> keys) {
		List<String> structuredKeys = new LinkedList();
		for (List<Object> key : keys) {
			structuredKeys.add((String) key.get(0));
		}
		return structuredKeys;
	}

}
