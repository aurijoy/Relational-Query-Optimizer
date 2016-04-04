package edu.buffalo.cse562.sqlparser;

import edu.buffalo.cse562.beans.SchemaBean;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class CreateTableParser {

	public CreateTableParser() {
		
	}
	public SchemaBean parseCreateTable(CreateTable statement){
		
		Table tableDef = statement.getTable();	
		System.out.println("Table name is "+tableDef.getWholeTableName());
		//System.exit(0);
		SchemaBean schema = new SchemaBean(tableDef.getWholeTableName().toLowerCase(), statement.getColumnDefinitions());
		return schema;
	}

}
