package edu.buffalo.cse562.raobjects;

import net.sf.jsqlparser.statement.select.Limit;

public class PrintRAObject extends BaseRAObject {
	
	public Limit limit;
	
	public PrintRAObject() {
		limit = new Limit();
		limit.setRowCount(Long.MAX_VALUE);
	}
	
	public PrintRAObject(Limit limit){
		this.limit=limit;		
	}
	

}
