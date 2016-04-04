package edu.buffalo.cse562.raobjects;

import edu.buffalo.cse562.interfaces.Operator;
import net.sf.jsqlparser.expression.Expression;

public class JoinRAObject extends RelationRAObject {
	
	public Expression onExp;
	public Operator ListOperator;
	
	public JoinRAObject(BaseRAObject parent) {
		super(parent);
		// TODO Auto-generated constructor stub
	}
	public JoinRAObject() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	
	

}
