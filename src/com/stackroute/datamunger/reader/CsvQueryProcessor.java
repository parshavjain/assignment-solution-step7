package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.parser.QueryParameter;

/**
 * This class read the data from csv.
 * Filter the fields based on the query
 * Note:  Filter the fields logic can be written in another class(util class), 
 * so can be used in other query processors.
 **/

public class CsvQueryProcessor implements QueryProcessingEngine{
	
	private DataSet dataSet;
	private List<String> record;
	//header contains key as field name and value as index
	private Map<String, Integer> header;
	//Filter is user defined class which is used to filter the fields, filter the records.
	private Filter filter;

	@Override
	public DataSet executeQuery(QueryParameter queryParameter) {
		
			//read header
			
			// read the remaining records
			
			
			//filter the fields based on query parameters.
			
		    return null;
	}

}
