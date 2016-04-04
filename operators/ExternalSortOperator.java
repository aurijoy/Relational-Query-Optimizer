package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.beans.SchemaBean;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;

public class ExternalSortOperator implements Operator {

	SchemaBean schema;
	BufferedReader fReader;
	List<Column> groupbyReferences;
	List<SelectItem> items;
	String Filename;
	
	public ExternalSortOperator( List<Column> groupbyReferences,List<SelectItem> items) {
		//this.schema = new SchemaBean();
		//this.schema.file = schema.file;
		this.schema = schema;
		this.groupbyReferences = groupbyReferences;
		this.items = items;
		reset();
	}
	public  void SortTuple(){
		this.reset();
		LeafValue[] tuple;
		int i=0;
		ArrayList<Integer> grp_list = new ArrayList<Integer>();
		for(SelectItem item:items){
			System.out.println("The items to be selected are "+item+" "+i);
			Expression exp = ((SelectExpressionItem)item).getExpression();
			
			for(Column col:groupbyReferences){
				System.out.println("Groupby references are "+ col);
				System.out.println("Expression to string is "+exp.toString());
				if(col.getWholeColumnName().equals(exp.toString()))
				{
					System.out.println("Column groupby is"+item);
					grp_list.add(i);
				}
			}
			i++;
		}
		
		do
		{
			tuple=this.getNext();
			
			
		}while(tuple!=null);
		
	}
	
	
	@Override
	public LeafValue[] getNext() {
		String line = null;
		try {
			line = fReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(line==null)
			return null;
		else{
			String[] data = line.split("\\|");
			LeafValue[] retVal = new LeafValue[data.length];
			List<ColumnDefinition> colNames = schema.colNames;
			for(int i=0;i<data.length;i++){
				String datatype = colNames.get(i).getColDataType().getDataType();
				if(datatype.equalsIgnoreCase(GlobalConstants.INTEGER_TYPE)
						||datatype.equalsIgnoreCase(GlobalConstants.LONG_TYPE)){					
					retVal[i] = new LongValue(Long.parseLong(data[i]));
				}else if(datatype.equalsIgnoreCase(GlobalConstants.STRING_TYPE)
						||datatype.equalsIgnoreCase(GlobalConstants.VARCHAR_TYPE)
						||datatype.equalsIgnoreCase(GlobalConstants.CHAR_TYPE)){					
					retVal[i] = new StringValue("'" + data[i] + "'");
				}else if(datatype.equalsIgnoreCase(GlobalConstants.DATE_TYPE)){		
					retVal[i] = new DateValue("'"+data[i]+"'");
				}
			}
			return retVal;
		}
		
	}

	@Override
	public void reset() {
		try {
			fReader = new BufferedReader(new FileReader(Filename+ GlobalConstants.DAT_SUFFIX));
		} catch (FileNotFoundException e) {
			System.err.println("Error in reset File");
			e.printStackTrace();
			fReader = null;
		}
		System.out.println("Resetting the relation"+this.toString());
		

	}

}
