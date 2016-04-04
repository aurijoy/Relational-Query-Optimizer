package edu.buffalo.cse562.eval;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import edu.buffalo.cse562.beans.SchemaBean;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import edu.buffalo.cse562.Eval;

public class Evaluator extends Eval{

	SchemaBean schema1;
	LeafValue[] tuple1;
	public Evaluator(SchemaBean schema1, LeafValue[] tuple) {
		this.schema1 = schema1;
		this.tuple1 = tuple;
	}

	@Override
	public LeafValue eval(Column col) throws SQLException {
		//\System.out.println(schema1.file.getName());
		//col.setTable(new Table("", schema1.file.getName()));
		System.out.println(col.getTable().getWholeTableName() + "." + col.getColumnName());
		String colName = col.getTable().getWholeTableName();
		if(colName=="" || colName==null){
			Iterator<Entry<String, Integer>> it = schema1.colIdx.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        String[] tab_col = ((String)pair.getKey()).split("\\.");
		        if(tab_col[1].equalsIgnoreCase(col.getColumnName())){
		        	col.getTable().setName(tab_col[0]);
		        	break;
		        }
		        //it.remove(); // avoids a ConcurrentModificationException
		        
		    }
		}
		if(schema1.jF1 == null && schema1.jF2==null){
			//IE: No Column Alias
			return (LeafValue)(tuple1[schema1.colIdx.get(schema1.file.getName() + "." + col.getColumnName())]);
		}else
			{
			System.out.println("Returning the leafvalue");
			System.out.println("Data is "+schema1.colIdx.get(col.getTable().getWholeTableName() + "." + col.getColumnName()));
			return (LeafValue)(tuple1[schema1.colIdx.get(col.getTable().getWholeTableName() + "." + col.getColumnName())]);
			}
		//return (LongValue)(tuple1[schema1.colIdx.get(schema1.file.getName() + "." + col.getColumnName())]);
		//return (LongValue)(tuple1[schema1.colIdx.get(schema1.colCols.get(col.getColumnName()).getTable().getWholeTableName() + "." + col.getColumnName())]);
	}

}
