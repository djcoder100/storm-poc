package com.kiku.gridlattice;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.kiku.gridlattice.timeline.HourlyTimeline;

/**
 * short "java script" (aha) to provide an easy to import csv file from R 
 *
 */
public class ExtractTimelines {
	
	public static void main(String[] args)  {
		
		System.out.println("dumping timelines from Cassandra to data/timelines.csv...");
		
		try (FileWriter fw = new FileWriter(new File("data/timelines.csv"))) {
			for (HourlyTimeline timeline : StormDB.DB.getAllTimelines()) {
				fw.write(timeline.toCsv() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("...done");
		
	}

}
