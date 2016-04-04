package edu.buffalo.cse562.operators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.beans.SchemaBean;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;

public class XProductOperator implements Operator{

	Operator rel1;
	Operator rel2;
	SchemaBean schema1;
	SchemaBean schema2;
	SchemaBean returnSchema;
	LeafValue[] cachedRel1Tuple;
	
	/**
	 * Hacky Iterator
	 * if true then read from rel1
	 * else
	 * read from rel2.
	 */
	boolean itr1or2 = true;
	
	public XProductOperator() {
		// TODO Auto-generated constructor stub
	}
	public XProductOperator(Operator rel1, Operator rel2, SchemaBean schema1,
			SchemaBean schema2, SchemaBean returnSchema) {
		this.rel1 = rel1;
		this.rel2 = rel2;
		this.schema1 = schema1;
		this.schema2 = schema2;
		List<ColumnDefinition> newColNames = new ArrayList<ColumnDefinition>();
		newColNames.addAll(schema1.colNames);
		newColNames.addAll(schema2.colNames);
		returnSchema = new SchemaBean(schema1.file.getName(), schema2.file.getName(),
				schema1.colNames,schema2.colNames,newColNames);
		this.returnSchema = returnSchema;
	}
	@Override
	public LeafValue[] getNext() {
		if(itr1or2){
			cachedRel1Tuple = rel1.getNext();
			if(cachedRel1Tuple==null)
				return null;
			rel2.reset();
			itr1or2 = !itr1or2;
			return this.getNext();
		}else{
			//Read from rel2
			LeafValue[] r2Tuple = rel2.getNext();
			if(r2Tuple==null){
				itr1or2 = !itr1or2;
				return this.getNext();
			}
			//Concat both rows
			LeafValue[] retVal = new LeafValue[returnSchema.colNames.size()];
			int rctr=0;
			int ctr = 0;
			do{
				retVal[rctr++] = cachedRel1Tuple[ctr++];
			}while(ctr<cachedRel1Tuple.length);
			//Reset ctr
			ctr = 0;
			do{
				retVal[rctr++] = r2Tuple[ctr++];
			}while(ctr<r2Tuple.length);
			return retVal;
		}
	}
	@Override
	public void reset() {
		return;
	}

}
