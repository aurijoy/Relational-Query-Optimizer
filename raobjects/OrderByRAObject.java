package edu.buffalo.cse562.raobjects;

import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

public class OrderByRAObject extends BaseRAObject{
	public List<OrderByElement> orderByElements;
	public List<SelectItem> items;
	public OrderByRAObject() {
		// TODO Auto-generated constructor stub
	}
	public OrderByRAObject(List<OrderByElement> orderByElements) {
		this.orderByElements = orderByElements;		
	}

}
