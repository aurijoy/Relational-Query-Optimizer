package edu.buffalo.cse562.interfaces;

import net.sf.jsqlparser.expression.LeafValue;

public interface Operator {
	public LeafValue[] getNext();
	public void reset();
	
}
