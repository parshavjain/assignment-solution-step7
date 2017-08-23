package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.parser.AggregateFunction;
import com.stackroute.datamunger.query.parser.QueryParameter;

//This class will read from CSV file and process and return the resultSet based on Aggregate functions
//Need to use Java 8 concepts like streams and filters
//Need to use Java 8 built in classes like IntSummaryStatistics OR DoubleSummaryStatistics
public class CsvAggregateQueryProcessor implements QueryProcessingEngine {

		private DataSet dataSet;
	    private List<AggregateFunction> aggregates;
			@Override
		public DataSet executeQuery(QueryParameter queryParameter) {
					//read header
					
					// read the remaining records
										
					// find min, max, sum and count based on given aggregate function
					return null;
			}
			
			/**
			 *This method is used the calculate aggregates like min, max, sum, avg, sum
			 * Used IntSummaryStatistics a java 8 built in class
			 * Used Streams and filters -  a java 8 concept
			 **/
			private List<AggregateFunction> calclulateAggregates(List<List<String>> result, Map<String, Integer> header,
					 			List<AggregateFunction> aggregates) {
					 		
					 			return null;
					 	}
	
	
}
