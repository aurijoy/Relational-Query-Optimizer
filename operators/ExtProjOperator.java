package edu.buffalo.cse562.operators;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.beans.SchemaBean;
import edu.buffalo.cse562.eval.Evaluator;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.interfaces.Operator;

public class ExtProjOperator implements Operator {
	Operator input;
	List<SelectItem> items;
	SchemaBean schema;
	Map<String, Double> aggregations;
	Map<String, Boolean> aggregationReturned;
	Map<String,List<LeafValue>> otherStuff;
	Boolean GroupbyFound;
	Double Min;
	public ExtProjOperator()  {
		aggregations = new HashMap<String, Double>();
		aggregationReturned = new HashMap<String, Boolean>();
	}

	public ExtProjOperator(Operator input, List<SelectItem> items, SchemaBean schema,Boolean GroupbyFound) {
		this.input = input;
		this.items = items;
		this.schema = schema;
		this.GroupbyFound=GroupbyFound;
		aggregations = new HashMap<String, Double>();
		aggregationReturned = new HashMap<String, Boolean>();
		otherStuff = new HashMap<String, List<LeafValue>>();
	}

	@Override
	public LeafValue[] getNext() {
		LeafValue[] tuple;
		LeafValue[] retVal = null;
		List<LeafValue> postTuple = null;
		postTuple = new ArrayList<LeafValue>();
		tuple = input.getNext();
		for(SelectItem item:items){
			System.out.println("Select items are "+item);
		}
		
		if(GroupbyFound)
		{
			System.out.println("Groupby found in the query");
		}
		
						
		LeafValue [] empty= new LeafValue[0];
		Boolean Agg_flag=false;
		if((GroupbyFound==false)&&(tuple==null)){
			/*if(!otherStuff.isEmpty()){
				for(LeafValue leaf:otherStuff)
					postTuple.add(leaf);
			}*/
			for(SelectItem item:items){
				if(item instanceof SelectExpressionItem){
					Expression exp = ((SelectExpressionItem)item).getExpression();
					if((exp instanceof Function)){
						Function func = (Function)exp;
						System.out.println("Function parameters are "+func.getParameters());
						System.out.println("The key being searched is "+func.getName()+func.getParameters().toString());
						//System.out.println("The add value is "+otherStuff.get(func.getName()+func.getParameters().toString()));
						//postTuple.addAll(otherStuff.get(func.getName()+func.getParameters().toString()));
						Agg_flag=false;
						if(func.getName().equalsIgnoreCase(GlobalConstants.SUM) && aggregationReturned.get(func.getName()+func.getParameters().toString())==false){
							System.out.println("Sum aggregation found");
							postTuple.add(new DoubleValue(aggregations.get(func.getName()+func.getParameters().toString())));
							aggregationReturned.put(func.getName()+func.getParameters().toString(), true);
						}
						else if(func.getName().equalsIgnoreCase(GlobalConstants.COUNT) && aggregationReturned.get(func.getName()+func.getParameters().toString())==false){
							postTuple.add(new DoubleValue(aggregations.get(func.getName()+func.getParameters().toString())));
							aggregationReturned.put(func.getName()+func.getParameters().toString(), true);
						}else if(func.getName().equalsIgnoreCase(GlobalConstants.AVG)){// && aggregationReturned.get(func.getName()+func.getParameters().toString())==false){
							postTuple.add(new DoubleValue((aggregations.get(func.getName()+func.getParameters().toString()+GlobalConstants.SUM)/(aggregations.get(func.getName()+func.getParameters().toString()+GlobalConstants.COUNT)))));
							//aggregationReturned.put(func.getName()+func.getParameters().toString(), true);
						}else if(func.getName().equalsIgnoreCase(GlobalConstants.MIN) && aggregationReturned.get(func.getName()+func.getParameters().toString())==false){
							postTuple.add(new DoubleValue(aggregations.get(func.getName()+func.getParameters().toString())));
							aggregationReturned.put(func.getName()+func.getParameters().toString(), true);
						}else if(func.getName().equalsIgnoreCase(GlobalConstants.MAX) && aggregationReturned.get(func.getName()+func.getParameters().toString())==false){
							postTuple.add(new DoubleValue(aggregations.get(func.getName()+func.getParameters().toString())));
							aggregationReturned.put(func.getName()+func.getParameters().toString(), true);
						}else{
							return null;
						}

					}else{
						continue;
						//return null;							
					}
					}
				}
			}
		else{
			postTuple = new ArrayList<LeafValue>();
			if(tuple == null)
				return tuple;
			for(SelectItem item:items){
				
				if(item instanceof AllColumns){
					//REturning all columns
					System.out.println("Sending out all the columns");
					System.out.println("Tuple is ");
					for (LeafValue leafValue : tuple) {
						System.out.println("Tuple value is "+leafValue.toString());
					}
					return tuple;
				}
				else if(item instanceof SelectExpressionItem){
					Evaluator eval = new Evaluator(schema, tuple);
					BooleanValue res = null;
					System.out.println("Inside the item selected");
					try {
						Expression exp = ((SelectExpressionItem)item).getExpression();
						if(exp instanceof Function){
							Function func = (Function)exp;
							ExpressionList expList=func.getParameters();
							if(GroupbyFound)
								{
									List<Expression> exp_list = expList.getExpressions();
									for (Expression expression : exp_list) {
										System.out.println("Expressions are "+expression);
										postTuple.add(eval.eval(expression));	
									}
									continue;
								}
							else
							{
							Agg_flag = true;								
							if(func.getName().equalsIgnoreCase(GlobalConstants.SUM)){
								Double sum = aggregations.get(func.getName()+func.getParameters().toString());
								System.out.println("The key being set in sum is "+func.getName()+func.getParameters().toString());								Double newSum = new Double(0);
								for(Expression param:((List<Expression>)func.getParameters().getExpressions())){									
									newSum+=((LongValue)eval.eval(param)).getValue();									
								}
								if(sum==null){
									
									aggregations.put(func.getName()+func.getParameters().toString(), newSum);
									aggregationReturned.put(func.getName()+func.getParameters().toString(), false);
									System.out.println("The sum values are "+newSum);
								}
								else
									aggregations.put(func.getName()+func.getParameters().toString(), sum+newSum);
									System.out.println("The sum values are "+newSum);
							}
							else if(func.getName().equalsIgnoreCase(GlobalConstants.COUNT)){
								Double count = aggregations.get(func.getName()+func.getParameters().toString());
								if(count==null){
									aggregations.put(func.getName()+func.getParameters().toString(), new Double(1));
									aggregationReturned.put(func.getName()+func.getParameters().toString(), false);
								}
								else
									aggregations.put(func.getName(), count+1);
							}
							else if(func.getName().equalsIgnoreCase(GlobalConstants.AVG)){
								Double sum = aggregations.get(func.getName()+func.getParameters().toString()+GlobalConstants.SUM);
								Double count = aggregations.get(func.getName()+func.getParameters().toString()+GlobalConstants.COUNT);
								double newSum = 0;
								for(Expression param:((List<Expression>)func.getParameters().getExpressions())){
									newSum+=((LongValue)eval.eval(param)).getValue();
								}
								if(sum==null){
									aggregations.put(func.getName()+func.getParameters().toString()+GlobalConstants.SUM, newSum);
									aggregations.put(func.getName()+func.getParameters().toString()+GlobalConstants.COUNT, new Double(1));
									aggregationReturned.put(func.getName(), false);
								}
								else{
									aggregations.put(func.getName()+func.getParameters().toString()+GlobalConstants.SUM, sum+newSum);
									aggregations.put(func.getName()+func.getParameters().toString()+GlobalConstants.COUNT, count+1);
									}
							}
							else if(func.getName().equalsIgnoreCase(GlobalConstants.MIN)){
								System.out.println("The key being set in min is "+func.getName()+func.getParameters().toString());
								Double min = aggregations.get(func.getName()+func.getParameters().toString());
								for(Expression param:((List<Expression>)func.getParameters().getExpressions())){									
									Double nextItem =((LongValue)eval.eval(param)).toDouble();
									System.out.println("The next item is "+nextItem);
									if(min==null){
										//this.Min=nextItem;
										//System.out.println("The value being set is "+nextItem);
										aggregations.put(func.getName()+func.getParameters().toString(), nextItem);
										aggregationReturned.put(func.getName()+func.getParameters().toString(), false);
									}
									else if(nextItem < min){
										this.Min=nextItem;
										min = nextItem;
										//System.out.println("The value being set is "+min);
										aggregations.put(func.getName()+func.getParameters().toString(), nextItem);
									}
									//aggregations.put(func.getName()+func.getParameters().toString(), this.Min);
								}
							}
							else if(func.getName().equalsIgnoreCase(GlobalConstants.MAX)){
								Double max = aggregations.get(func.getName()+func.getParameters().toString());
								for(Expression param:((List<Expression>)func.getParameters().getExpressions())){
									Double nextItem =((LongValue)eval.eval(param)).toDouble();
									if(max==null){
										aggregations.put(func.getName()+func.getParameters().toString(), nextItem);
										aggregationReturned.put(func.getName()+func.getParameters().toString(), false);
									}
									else if(nextItem > max){
										max = nextItem;
										aggregations.put(func.getName()+func.getParameters().toString(), nextItem);
									}
								}
							}
							/*for (Iterator<LeafValue> it = postTuple.iterator(); it.hasNext(); ) {
								LeafValue leaf = it.next();
								
								it.remove();
							}*/
							System.out.println("Reaching here");
							otherStuff.put(func.getName()+func.getParameters().toString(), postTuple);
							for (LeafValue val : postTuple)
							{
								System.out.print(val.toString()+"|");
							}
							System.out.println("");							
							postTuple = new ArrayList<LeafValue>();
							System.out.println("Empt length is "+empty.length);
							
						}					
						}
						else{
							System.out.println("Adding to the evaluate");
							LeafValue ret_val = eval.eval(exp);
							if(ret_val==null)
							{
								break;
							}
							postTuple.add(ret_val);	
						}
						
						
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//System.out.println(res.toString());
					//retVal.add()
				}
			}
		}
		
			if(Agg_flag)
				return empty;
			retVal = new LeafValue[postTuple.size()];
			int count = 0;
			for(LeafValue val:postTuple){
				retVal[count++]=val;
			}
			
			/*for (LeafValue val : retVal) {
				//System.out.println("Tuple is "+leafValue.toString());
				
					if(val instanceof StringValue){
						String stringVal = val.toString();
						System.out.print(stringVal.substring(1, stringVal.length()-1) + "|");
					}else
						System.out.print(val+ "|");
			}*/
			System.out.println("");
		//if(retVal.length==items.size())
			System.out.println("Returning the the tuple to upper layer");
			
			return retVal;
	}

	@Override
	public void reset() {
		
	}

}
