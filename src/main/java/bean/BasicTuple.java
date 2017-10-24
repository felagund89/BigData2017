package main.java.bean;

import java.io.Serializable;

public class BasicTuple implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 7356936525061749503L;

	
	String subject;
	
	String object;
	
	String predicate;
	
	String optional;

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getObject() {
		return object;
	}

	public void setObject(String object) {
		this.object = object;
	}

	public String getPredicate() {
		return predicate;
	}

	public void setPredicate(String predicate) {
		this.predicate = predicate;
	}

	public String getOptional() {
		return optional;
	}

	public void setOptional(String optional) {
		this.optional = optional;
	}
	
	
}
