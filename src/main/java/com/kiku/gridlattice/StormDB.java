package com.kiku.gridlattice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.*;
import org.codehaus.jackson.map.ObjectMapper;

import com.kiku.gridlattice.period.RoomPresencePeriod;
import com.kiku.gridlattice.timeline.HourlyTimeline;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormDB {

    private static final Logger LOGGER = LoggerFactory.getLogger(StormDB.class);

    // quick and dirty JVM-wide singleton
	public final static StormDB DB = new StormDB();
    ObjectMapper mapper = new ObjectMapper();
    PreparedStatement statement_presence = CassandraConnection.getSession().prepare("INSERT INTO presence  (id, payload) values (?,?)");
    PreparedStatement statement_roomtimelines = CassandraConnection.getSession().prepare("INSERT INTO room_timelines  (id, timeline) values (?,?)");

	//////////////
	// room presence periods
	//////////////
	
	public List<RoomPresencePeriod> getPresencePeriods(List<String> correlationIds) {

		// using just the correlation provided in the event as Cassandra id (brr....) 
		ResultSet rs = CassandraConnection.execute("select id, payload from presence where id in ( " + toCsv(correlationIds) + " ) ");

		Map<String, RoomPresencePeriod> fromDB = unmarshallResultSet(rs, RoomPresencePeriod.class, "payload");

		// this ensures we return a list of the same size as the one received as input (may contain null values)
		List<RoomPresencePeriod> result = new ArrayList<RoomPresencePeriod>(correlationIds.size());
		for (String corrId : correlationIds) {
			result.add(fromDB.get(corrId));
		}

		return result;
	}

	public void upsertPresencePeriods(List<RoomPresencePeriod> periods) {
		for (RoomPresencePeriod rpp : periods) {
			try {
				String periodJson = mapper.writeValueAsString(rpp);
                CassandraConnection.execute(new BoundStatement(statement_presence).bind(rpp.getId(), periodJson));
			}
            catch (Exception ex) {
                LOGGER.error("Failed to upsertPresencePeriods", ex);
			}
		}
	}


    /**
     * Timelines can be added here
     * @param roomIdAndStartTime
     * @return
     */
	public List<HourlyTimeline> getTimelines(List<Pair<String, Long>> roomIdAndStartTime) {

		List<String> ids = new ArrayList<>(roomIdAndStartTime.size());
		for (Pair<String, Long> roomidAndStartTime : roomIdAndStartTime) {
			ids.add(buildTimelineId(roomidAndStartTime.first, roomidAndStartTime.second));
		}

		ResultSet rs = CassandraConnection.execute("select id, timeline from room_timelines where id in ( " + toCsv(ids) + " ) ");
		Map<String, HourlyTimeline> fromDB = unmarshallResultSet(rs, HourlyTimeline.class, "timeline");

		// this ensures we return a list of the same size as the one received as input (may contain null values)
		List<HourlyTimeline> result = new ArrayList<>(roomIdAndStartTime.size());
		for (String id : ids) {
			result.add(fromDB.get(id));
		}

		return result;
	}
	
	
	public Collection<HourlyTimeline> getAllTimelines() {
		ResultSet rs = CassandraConnection.execute("select id, timeline from room_timelines ");
		return unmarshallResultSet(rs, HourlyTimeline.class, "timeline").values();
	}

	public void upsertTimelines(List<HourlyTimeline> timelines) {
		for (HourlyTimeline timeline : timelines) {
			try {
				String id = buildTimelineId(timeline.getRoomId(), timeline.getSliceStartMillis());
				String timelineJson = mapper.writeValueAsString(timeline);

                CassandraConnection.execute(new BoundStatement(statement_roomtimelines).bind(id, timelineJson));
			}
            catch (Exception ex) {
                LOGGER.error("Failed to upsertTimelines", ex);
			}
		}
	}

	private String buildTimelineId(String roomId, Long startTime) {
		return roomId + "_" + (long) Math.ceil(startTime);
	}

	//////////////
	// generic DB stuff
	//////////////
	public void reset() {

		recreateTable("presence", "(id text PRIMARY KEY, payload TEXT)");
		recreateTable("room_timelines", "(id text PRIMARY KEY, timeline TEXT)");
	}

	private void recreateTable(String tableName, String spec) {
		try {
            CassandraConnection.execute("drop table " + tableName);
		}
        catch (InvalidQueryException ex) {
            LOGGER.error("warning: could not drop table " + tableName + ", is the code executed for the first time?", ex);
		}
        CassandraConnection.execute("create table  " + tableName + " " + spec);
	}

	public void close() {
		CassandraConnection.close();
	}


	// /////////////////////////////////
	// some utility methods

	private String toCsv(List<String> vals) {
		StringBuffer stb = new StringBuffer();
		boolean first = true;
		for (String val : vals) {
			if (!first) {
				stb.append(" , ");
			}
			stb.append(" '").append(val).append("' ");
			first = false;
		}
		return stb.toString();
	}

	/**
	 * expects this resultSet to contain a "id" and a json field called fieldName => unmarshalls that and return a map
	 */
	private <T> Map<String, T> unmarshallResultSet(ResultSet resultSet, Class<T> expectedClass, String fieldName) {

		Map<String, T> fromDB = new HashMap<String, T>();
		while (!resultSet.isExhausted()) {
			try {
				Row row = resultSet.one();
				T unmarshalled = mapper.readValue(row.getString(fieldName), expectedClass);
				fromDB.put(row.getString("id"), unmarshalled);
			}
            catch (Exception ex) {
                LOGGER.error("Failed unmarshallResultSet", ex);
			}
		}
		return fromDB;
	}
}