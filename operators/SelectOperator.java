package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import edu.buffalo.cse562.beans.SchemaBean;
import edu.buffalo.cse562.eval.Evaluator;
import edu.buffalo.cse562.interfaces.Operator;

public class SelectOperator implements Operator{
	Operator input;
	Expression exp;
	SchemaBean schema;
	
	//EXP: Where Clause.
	public SelectOperator(Operator input, Expression exp, SchemaBean schema) {
		this.input = input;
 		this.exp = exp;
 		this.schema = schema;
 	}

	@Override
	public LeafValue[] getNext() {
		LeafValue[] tuple;
		LeafValue[] retVal = null;
		do{
			tuple = input.getNext();
			if(tuple==null)
				return null;
			else{
				Evaluator lt = new Evaluator(schema, tuple);
				try {
					System.out.println("Class of exp is "+exp.getClass());
					System.out.println("EXP is "+exp);
					System.out.println("Evaluated string is "+lt.eval(exp));
					//System.out.println(x);
					
					BooleanValue result = (BooleanValue) lt.eval(exp);
					//LeafValue result = lt.eval(exp);
					//System.out.println("Return value is "+result.toString());
					//System.exit(0);
					if(result.getValue())
						retVal = tuple;
				} catch (SQLException e) {
					System.err.println("Error in Eval");
					e.printStackTrace();	
				}
			}
		}while(retVal==null);
		//LeafValue[] retVal = new LeafValue[count];
		//count=0;
		//for(LeafValue[] val:retTuple)
		//	retVal[count++] = val;

		return retVal;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
