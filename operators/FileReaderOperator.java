package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.beans.SchemaBean;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;

public class FileReaderOperator implements Operator {

	SchemaBean schema;
	BufferedReader fReader;
	
	public FileReaderOperator(SchemaBean schema) {
		//this.schema = new SchemaBean();
		//this.schema.file = schema.file;
		this.schema = schema;
		reset();
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
					//System.out.println("Date value is "+data[i]);
					retVal[i] = new DateValue("'"+data[i]+"'");
				}else if(datatype.equalsIgnoreCase(GlobalConstants.DECIMAL_TYPE)){		
					retVal[i] = new DoubleValue(Double.parseDouble(data[i]));
				}
				
			}
			return retVal;
		}
		
	}

	@Override
	public void reset() {
		try {
			//System.out.println("File is "+schema.file.getPath());
			fReader = new BufferedReader(new FileReader(schema.file.getPath()));
		} catch (FileNotFoundException e) {
			System.err.println("Error in reset File");
			e.printStackTrace();
			fReader = null;
		}
		//System.out.println("Resetting the relation"+this.toString());
		

	}
	
	public void close(){
		try {
			this.fReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
