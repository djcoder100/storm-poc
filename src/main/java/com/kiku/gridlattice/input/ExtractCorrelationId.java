package com.kiku.gridlattice.input;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import com.kiku.gridlattice.LocationChangedEvent;

public class ExtractCorrelationId extends BaseFunction {
	public void execute(TridentTuple tuple, TridentCollector collector) {
		LocationChangedEvent event = (LocationChangedEvent) tuple.getValueByField("occupancyEvent");
		collector.emit(new Values(event.getCorrId()));
	}
}
