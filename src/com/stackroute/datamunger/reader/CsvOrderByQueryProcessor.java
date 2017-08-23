package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.stackroute.datamunger.query.DataSet;
import com.stackroute.datamunger.query.Filter;
import com.stackroute.datamunger.query.parser.QueryParameter;

/**
 * This class will read data from CSV file and sort based on order by field.
 * 
 */
public class CsvOrderByQueryProcessor implements QueryProcessingEngine{
	
	private List<String> record;
	private Filter filter;
	
	@Override
	public DataSet executeQuery(QueryParameter queryParameter) {
			//read header
			
			// read the remaining records
			           
		
		return null;
	}

	private List<List<String>> sortRecords(QueryParameter queryParameter, List<List<String>> result) {
		
		
		return null;
	}


}
