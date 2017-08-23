package com.stackroute.datamunger.query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.reader.CsvAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByQueryProcessor;
import com.stackroute.datamunger.reader.CsvOrderByQueryProcessor;
import com.stackroute.datamunger.reader.CsvQueryProcessor;
import com.stackroute.datamunger.reader.CsvWhereQueryProcessor;
import com.stackroute.datamunger.reader.QueryProcessingEngine;

/**
 * This class is used to execute the query with the help of different Query Processors.
 * Decide what type of Query processors you required.
 * Hint:  For each different query type you need different Query Procesor.
 * You can write any private methods if required(to be used in this class only)
 **/
public class Query {

	
	public DataSet executeQuery(String queryString) {
		
	
		// checking type of Query
	
		
		// queries without aggregate functions, order by clause or group by clause
		
		
		//queries with aggregate functions
		
		
		//Queries with group by clause
		
		return null;
	}
	
	
	private void setHeader() {
		
			// read the header record
		
	}

	private void setSelectedFieldIndex() {
	
	}

}
