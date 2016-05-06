package com.conveyal.gtfs.validator.model;

import com.conveyal.gtfs.model.Route;

import java.io.Serializable;

public class InvalidValue implements Serializable {

	public String affectedEntity;
	
	public String affectedField;
	
	public String affectedEntityId;
	
	public String problemType;
	
	public String problemDescription;
	
	/** Is this something that is high priority or a nice-to-have? */
	public Priority priority;
	
	public Object problemData;
	
	/** The route affected by this issue */
	public Route route;
	
	@Deprecated
	/**
	 * Create a new record of an invalid value. This function is deprecated in favor of the form that takes a priority as well.
	 */
	public InvalidValue(String affectedEntity,  String affectedField, String affectedEntityId, String problemType, String problemDescription, Object problemData) {
		this(affectedEntity, affectedField, affectedEntityId, problemType, problemDescription, problemData, Priority.UNKNOWN);
	}
	
	public InvalidValue(String affectedEntity,  String affectedField, String affectedEntityId, String problemType,
	        String problemDescription, Object problemData, Priority priority) {
		this.affectedEntity = affectedEntity;
		this.affectedField = affectedField;
		this.affectedEntityId = affectedEntityId;
		this.problemType =  problemType;
		this.problemDescription = problemDescription;
		this.problemData = problemData;
		this.priority = priority;
	}
	
	public String toString() {
		
		return problemType + "\t" + affectedEntityId + ":\t"  + problemDescription;
		
	}
	
}
