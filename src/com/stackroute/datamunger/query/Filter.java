package com.stackroute.datamunger.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.Restriction;

//This class is like utility class.
//We can define common methods which can be used acrross the project.
//Like filterFields, isRequiredRecord etc.,
//You can write private methods to do subtask of the above methods.
public class Filter {
	
	
	
	/**
	 * This method take the record and remove the fields from the record.
	 * The record consist of all the fields, you have to keep what use selected in the query.
	 */
	public List<String> filterFields(QueryParameter queryParameter, List<String> record) {
		
		return null;
	}
	
	
	/**
	 *This method takes record as input and return true if the record is required based on 'where'  condition given in the query.
	 * Multiple conditions may be exist in "where" part of the query.  
	 **/
	public boolean isRequiredRecord(QueryParameter queryParameter, List<String> record) {
		
		return true;
	}
	
	/**
	 * This method return true if the record is required 'where' condition
	 * If not it returns false.
	 **/
	private boolean evaluateRelationalExpression(List<String> record, Restriction restriction) {
		
		return true;
	}

}
