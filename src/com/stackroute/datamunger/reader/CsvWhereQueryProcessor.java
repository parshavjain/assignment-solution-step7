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
 *This class will read from CSV file and process and return the resultSet based on Where clause
*Filter is user defined utility class where we defined the methods related to filter fields,
* filter records.
 **/
public class CsvWhereQueryProcessor implements QueryProcessingEngine{

	private DataSet dataSet;
	private List<String> record;
	private Map<String, Integer> header;
	private Filter filter;

/**
 *This method used to read the data from csv and filter the fields with the help of  Filter class.
 **/
	public DataSet executeQuery(QueryParameter queryParameter) {
			// read header
			
			// read the remaining records
			

            //Check wither the record is required or not based on 'where' coniditin in the query parameter
			return null;
	}

}
