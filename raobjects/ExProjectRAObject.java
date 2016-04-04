package edu.buffalo.cse562.raobjects;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ExProjectRAObject extends BaseRAObject{

	public List<SelectItem> items;
	public Limit limit;
	public Boolean GroupbyFound;
	public ExProjectRAObject() {
		limit = new Limit();
		limit.setRowCount(Long.MAX_VALUE);
	}

}
