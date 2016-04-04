package edu.buffalo.cse562.raobjects;

import net.sf.jsqlparser.expression.Expression;
import edu.buffalo.cse562.interfaces.Operator;

public class HybridHashRAObject extends RelationRAObject{
	public Expression onExp;
	public Operator ListOperator;
	public String[] joiningattributes;
	public HybridHashRAObject(BaseRAObject parent) {
		super(parent);
		// TODO Auto-generated constructor stub
	}
	public HybridHashRAObject() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	
	


}
