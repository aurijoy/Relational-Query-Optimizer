package edu.buffalo.cse562.raobjects;

import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

public class GroupByRAObject extends BaseRAObject{
	public List<Column> columnReferences;
	public List<SelectItem> items;
	public GroupByRAObject() {
		// TODO Auto-generated constructor stub
	}
	public GroupByRAObject(List<Column> columnReferences) {
		this.columnReferences = columnReferences;
	}

}
