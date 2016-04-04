package edu.buffalo.cse562.sqlparser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse562.globals.GlobalConstants.RAOperator;
import edu.buffalo.cse562.operators.ExternalSortOperator;
import edu.buffalo.cse562.operators.PrintRAOperator;
import edu.buffalo.cse562.raobjects.AggregateRAObject;
import edu.buffalo.cse562.raobjects.BaseRAObject;
import edu.buffalo.cse562.raobjects.ExProjectRAObject;
import edu.buffalo.cse562.raobjects.GroupByRAObject;
import edu. buffalo.cse562.raobjects.JoinRAObject;
import edu.buffalo.cse562.raobjects.OrderByRAObject;
import edu.buffalo.cse562.raobjects.PrintRAObject;
import edu.buffalo.cse562.raobjects.RelationRAObject;
import edu.buffalo.cse562.raobjects.UnionRAObject;
import edu.buffalo.cse562.raobjects.WhereRAObject;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserTokenManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.Union;

public class SelectParser {

	public SelectParser() {
		
	}
	public void explist(ArrayList<Expression>data,Expression test) {
				if(test instanceof AndExpression)
				{
					explist(data, ((AndExpression) test).getLeftExpression());
					explist(data, ((AndExpression) test).getRightExpression());
				}
				else
				{
					data.add(test);
				}		
	}
	
	public UnionRAObject getSelectsFromUnion(SelectBody selBody){
		Union union = (Union) selBody;
		UnionRAObject unions = new UnionRAObject();
		unions.operator = RAOperator.UNION;
		for(PlainSelect select:((List<PlainSelect>)union.getPlainSelects())){
			BaseRAObject head = null;
			head = createRAFromSelect(select);
			unions.statementHeads.add(head);
		}
		return unions;
	}
	public BaseRAObject createRAFromSelect(Select statement){
		PlainSelect stmt = (PlainSelect)statement.getSelectBody();
		return createRAFromSelect(stmt);
	}
	public BaseRAObject createRAFromSelect(PlainSelect stmt){
		BaseRAObject treeHead = new BaseRAObject();
		BaseRAObject retHead = treeHead;
		Boolean GroupByFound= false;
		PrintRAObject printRA = new PrintRAObject(stmt.getLimit());
		printRA.parent = treeHead;
		
		if(treeHead.leftChild==null)
			treeHead.leftChild = printRA;
		else
			treeHead.rightChild = printRA;
		printRA.operator = RAOperator.PRINT;
		printRA.limit = stmt.getLimit();
		treeHead=printRA;
		System.out.println("Setting the print operator");
		if(stmt!=null){
			if(stmt.getDistinct()!=null){
				System.out.println("Distinct keyword found");
			}
			
			if(stmt.getOrderByElements()!=null)
			{
				System.out.println("Order by elements found "+stmt.getOrderByElements());
				List<OrderByElement> test = stmt.getOrderByElements();
				for (OrderByElement ordByElement : test) {					
					System.out.println("Orderby class is "+ordByElement.getExpression());
				}
				//System.exit(0);
				OrderByRAObject orderByRA = new OrderByRAObject(test);
				orderByRA.parent = treeHead;
				orderByRA.operator = RAOperator.ORDER_BY;
				orderByRA.items = stmt.getSelectItems();
				if(treeHead.leftChild==null)
					treeHead.leftChild = orderByRA;
				else
					treeHead.rightChild = orderByRA;
				treeHead=orderByRA;	
				//System.exit(0);
			}
			
			if(stmt.getHaving()!=null)
			{
				System.out.println("Having condition found");
			}
	
			//HAndle groupby
			if(stmt.getGroupByColumnReferences()!=null){
				
				System.out.println("Group by references found");
				System.out.println("Group by references are "+stmt.getGroupByColumnReferences());
				GroupByRAObject groupByRA = new GroupByRAObject(stmt.getGroupByColumnReferences());
				List<SelectItem> selectItems = stmt.getSelectItems();				
				groupByRA.parent = treeHead;
				groupByRA.operator = RAOperator.GROUP_BY;
				groupByRA.items = selectItems;
				if(treeHead.leftChild==null)
					treeHead.leftChild = groupByRA;
				else
					treeHead.rightChild = groupByRA;
				treeHead = groupByRA;				
				GroupByFound = true;
				/*for(Column col:stm){
					System.out.println("Groupby references are "+ col);
					System.out.println("Expression to string is "+exp.toString());
					String Tablename = col.getTable().getName();
					/*if(col.getWholeColumnName().equals(exp.toString()))
					{
						System.out.println("Column groupby is"+item);
						grp_list.add(i);
					}*/
					//break;
				//}
				//ExternalSortOperator eSortOperator = new ExternalSortOperator(stmt.getGroupByColumnReferences(), selectItems);
			}
			
							
			
			//Handle SELECTION
			if(stmt.getWhere()!=null){								
				System.out.println("The where clause is "+stmt.getWhere());				
		        Expression expr = stmt.getWhere();
		        
		        ArrayList<Expression> expr_list= new ArrayList<Expression>();
		        explist(expr_list, expr);
		        for (Expression expression : expr_list) {
					System.out.println("Expression is "+expression);
					WhereRAObject selectObj = new WhereRAObject(treeHead);
					selectObj.operator = RAOperator.SELECT;
					selectObj.parent = treeHead;
					selectObj.exp = expression;
					treeHead.leftChild = selectObj;
					treeHead = selectObj;
				}		      
			}
			
			//Handle Project
			if(stmt.getSelectItems()!=null){
				List<SelectItem> selectItems = stmt.getSelectItems();
				//Handle Aggregate
				//Handle Project
				for (SelectItem selectItem : selectItems) {
					System.out.println("Select iterms are "+selectItem);
				}
				
				ExProjectRAObject exProjObj = new ExProjectRAObject();
				exProjObj.parent = treeHead;
				exProjObj.operator = RAOperator.EXTENDED_PROJECT;
				exProjObj.items = selectItems;
				exProjObj.GroupbyFound =GroupByFound;
				if(stmt.getLimit() != null){
					exProjObj.limit = stmt.getLimit();
				}
				treeHead.leftChild = exProjObj;
				treeHead = exProjObj;
			}
						
			//Handle Relations
			if(stmt.getFromItem()!=null){
				List<Join> joins = stmt.getJoins();
				if(joins!=null)
					Collections.reverse(joins);
				System.out.println("The joins are "+joins);
				//System.exit(0);
				if(joins!=null){
					JoinRAObject join = new JoinRAObject(treeHead);
					join.operator = RAOperator.JOIN;
					Iterator<Join> joinItr = joins.iterator();
					while(joinItr.hasNext()){
						Join nextRel = joinItr.next();
						System.out.println("Next relation is "+nextRel.getRightItem());
						RelationRAObject relation = new RelationRAObject(join,((Table)nextRel.getRightItem()).getWholeTableName());
						if(join.rightChild==null)
							{
							System.out.println("Adding as right child "+relation.tablename);
							join.rightChild = relation;
							}
						else if(join.leftChild==null)
							{System.out.println("Adding as left child "+relation.tablename);
							join.leftChild = relation;
							}
						else{
							JoinRAObject subJoin = new JoinRAObject(join);
							subJoin.operator = RAOperator.JOIN;
							subJoin.rightChild = relation;
							System.out.println("Adding as right child "+relation.tablename);
							subJoin.leftChild = join;
							join.parent = subJoin;
							join = subJoin;
						}	
					}
					//Set the from item
					RelationRAObject relation = new RelationRAObject(join,((Table)stmt.getFromItem()).getWholeTableName());
					if(join.rightChild==null)
					{
						System.out.println("Adding as right child "+relation.tablename);
						join.rightChild = relation;
					}
					else if(join.leftChild==null)
						{
						System.out.println("Adding as right child "+relation.tablename);
						join.leftChild = relation;
						}
					else{
						JoinRAObject subJoin = new JoinRAObject(join);
						subJoin.operator = RAOperator.JOIN;
						subJoin.rightChild = relation;
						subJoin.leftChild = join;
						join.parent = subJoin;
						join = subJoin;
						System.out.println("Adding as right child "+relation.tablename);
					}
					treeHead.leftChild = join;
				}else{
					
					treeHead.rightChild = new RelationRAObject(treeHead,((Table)stmt.getFromItem()).getWholeTableName());
				}
				//System.exit(0);
			}
			
		}
		while(treeHead.parent!=null){
			treeHead = treeHead.parent;
		}
		return treeHead.leftChild;
	}

}
