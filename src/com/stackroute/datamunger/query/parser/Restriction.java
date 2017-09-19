package com.stackroute.datamunger.query.parser;

/**
 * This class is the custom class to hole 'where' condition detais.
 * 'Where' conditin  contains 3 items called properName, propertyValue and condition
 * So create private fields and generate getter/setter methods.
 * private String propertyName;
 * private String propertyValue;
 * private String condition;
 * 
 * Note:  This is POJO class(A class which has properties and getter/setter methods)
 * No other logic should present in this class.
 * 
 * */
public class Restriction {
	

	
	private String propertyName;
	private String propertyValue;
	private String condition;
	public Restriction(String propertyName, String propertyValue, String condition) {
		this.propertyName = propertyName;
		this.propertyValue = propertyValue;
		this.condition = condition;
	}
	public String getPropertyName() {
		return propertyName;
	}
	public String getPropertyValue() {
		return propertyValue;
	}
	public String getCondition() {
		return condition;
	}


	
	
}
