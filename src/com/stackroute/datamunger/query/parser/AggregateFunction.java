package com.stackroute.datamunger.query.parser;

/**
 * This class used to store aggregate function details.
 * Declare the following private properties and then generate getter/setter
 * 
 * private String field;  - to know on what field we need to calculate aggregates
 * private int result;     - to store the aggregate result
 * private String function; - to store what the is the aggregate function( sum, avg, min, max, count)
 * 
 **/

public class AggregateFunction {

	
	private String field;

	private int result;

	private String function;

	private int aggregateFieldIndex;

	public int getAggregateFieldIndex() {
		return aggregateFieldIndex;
	}

	public void setAggregateFieldIndex(int aggregateFieldIndex) {
		this.aggregateFieldIndex = aggregateFieldIndex;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public int getResult() {
		return result;
	}

	public void setResult(int result) {
		this.result = result;
	}

	public String getFunction() {
		return function;
	}

	public void setFunction(String function) {
		this.function = function;
	}
	



}
