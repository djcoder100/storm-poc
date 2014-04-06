package com.kiku.gridlattice.timeline;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import com.kiku.gridlattice.period.RoomPresencePeriod;

/**
 * Filters out any period with missing start time or end time
 *
 */
public class IsPeriodComplete extends BaseFilter {

	public boolean isKeep(TridentTuple tuple) {
		RoomPresencePeriod period = (RoomPresencePeriod) tuple.getValueByField("presencePeriod");
		return period.getStartTime() != null && period.getEndTme() != null;
	}

}
