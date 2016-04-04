package edu.buffalo.cse562.operators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import javax.management.relation.Relation;

import edu.buffalo.cse562.beans.SchemaBean;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.globals.GlobalConstants.RAOperator;
import edu.buffalo.cse562.interfaces.Operator;
import edu.buffalo.cse562.raobjects.BaseRAObject;
import edu.buffalo.cse562.raobjects.ExProjectRAObject;
import edu.buffalo.cse562.raobjects.GroupByRAObject;
import edu.buffalo.cse562.raobjects.HybridHashRAObject;
import edu.buffalo.cse562.raobjects.JoinRAObject;
import edu.buffalo.cse562.raobjects.OrderByRAObject;
import edu.buffalo.cse562.raobjects.PrintRAObject;
import edu.buffalo.cse562.raobjects.RelationRAObject;
import edu.buffalo.cse562.raobjects.UnionRAObject;
import edu.buffalo.cse562.raobjects.WhereRAObject;
import edu.buffalo.cse562.sqlparser.CreateTableParser;
import edu.buffalo.cse562.sqlparser.SelectParser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

public class SqlReader {
	Map<String,SchemaBean> relations = new HashMap<String, SchemaBean>(); 
	List<BaseRAObject> relationalAlgebra = new ArrayList<BaseRAObject>();
	public SqlReader() {
		// TODO Auto-generated constructor stub
	}
	
	public void parseAndExecute(String fileName) throws ParseException{
		try {
			
			CreateTableParser ctParser = new CreateTableParser();
			BufferedReader fReader = new BufferedReader(new FileReader(fileName));
			CCJSqlParser parser = new CCJSqlParser(new FileReader(fileName));			
		    Statement s;
		    SelectParser selParser = new SelectParser();
		    while((s = parser.Statement()) != null){
		        
		        // Figure out what kind of statement we've just encountered

		        if(s instanceof CreateTable){		         
		        	System.out.println("Create table statement found ");
		        	System.out.println("Create table statement is "+s.toString());
		        	SchemaBean table = ctParser.parseCreateTable((CreateTable)s);	
		        	System.out.println("Putting the tablename as "+table.file.getName());
		        	String str = table.file.getName().substring(0, table.file.getName().lastIndexOf("."));		     
					relations.put(str.toLowerCase(), table);

		        } else if(s instanceof Select) {
		        	System.out.println("Select statement found");
		        	System.out.println("Select statement is "+s.toString());
		        	SelectBody selBody = ((Select)s).getSelectBody();
					if(selBody instanceof Union){
						relationalAlgebra.add(selParser.getSelectsFromUnion(selBody));
					}
					else if(selBody instanceof PlainSelect){
					relationalAlgebra.add(selParser.createRAFromSelect(((Select) s)));
					}
		        }
		    }
			//CreateTableParser ctParser = new CreateTableParser();
			System.out.println("Inside the create table parser");
			//SelectParser selParser = new SelectParser();			
			
		} catch (FileNotFoundException e) {
			System.err.println("Error in reading File");
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		processRelationalAlgebra(relationalAlgebra);
	}
	
	
	void processRelationalAlgebra(List<BaseRAObject> relationalAlgebra){
		long rowsOutput = 0;
		try{
			for(BaseRAObject head:relationalAlgebra){
				
				//System.out.println("------------------------------------------------------------------");
				Stack<BaseRAObject> operators = new Stack<BaseRAObject>();				
				LeafValue[] tuple = null;
				Operator prevOp = null;
				SchemaBean schema = null;
				//Do Pre-order traversal and push onto stack
				System.out.println("Getting RA from tree");
				getStackFromRATree(operators,head);
				
				Stack<BaseRAObject> refactor = new Stack<BaseRAObject>();
				Stack<BaseRAObject> copy_op = (Stack<BaseRAObject>) operators.clone();
				//Get all the equality operators
				ArrayList<Expression> equal_op = new ArrayList<Expression>();				
				int i=0;
				for (BaseRAObject baseRAObject : operators) {
					System.out.println(baseRAObject.operator);										
					
					if(baseRAObject.operator.compareTo(RAOperator.SELECT)==0)
					{
						WhereRAObject sel = (WhereRAObject)baseRAObject;
						Expression pass = sel.exp;
						System.out.println("Expression is "+pass);
						Expression exp_data;
						if(pass instanceof Parenthesis)
						{
							Parenthesis parathesis= (Parenthesis) pass;
							exp_data = parathesis.getExpression();
						}
						else
						{
							exp_data = pass;
						}
					
						System.out.println("Expression inside is "+exp_data);
						System.out.println("The expression type is "+pass.getClass());
						
						if(exp_data instanceof EqualsTo)
							{
								EqualsTo equalsTo = (EqualsTo) exp_data;
								Expression left = equalsTo.getLeftExpression();
								Expression right = equalsTo.getRightExpression();
								if((left instanceof Column) && (right instanceof Column))
								{
									System.out.println("The selection can be pushed to project");
									copy_op.remove(i);
									//Adding equijoin condition
									equal_op.add(exp_data);
									refactor.push(baseRAObject);
								}
								else
								{
									refactor.push(baseRAObject);
								}
								System.out.println("The expression has equal to");
								System.out.println("Right expression is "+right);
								System.out.println("Expression class is "+right.getClass());
								System.out.println("Left expression is "+left);
								System.out.println("Expression class is "+left.getClass());
							}
							else 
							{
								refactor.push(baseRAObject);
							}
						System.out.println("Expression is "+pass);
					}
					else 
					{
						refactor.push(baseRAObject);
					}
					i++;
				}
				
				System.out.println("");
				//Stack after refactoring 
				System.out.println("\nRefactored stack");
				for (BaseRAObject baseRAObject2 : refactor) {
					System.out.println(baseRAObject2.operator);
				}				
				System.out.println("Expressions are :");
				for (Expression expression : equal_op) {
					System.out.println("Exp:"+expression);
				}
				
				//Copy the refactored RA
				Stack<BaseRAObject> refactor_copy = (Stack<BaseRAObject>) refactor.clone();
				Stack<BaseRAObject> refactor_copy1 = (Stack<BaseRAObject>) refactor.clone();
				i=0;
				ArrayList<Expression> exp_to_be_removed = new ArrayList<Expression>();
				for (BaseRAObject baseRAObject2 : refactor) {
					System.out.println(baseRAObject2.operator);
					
					if(baseRAObject2.operator.compareTo(RAOperator.JOIN)==0){
						JoinRAObject joinRA = (JoinRAObject) baseRAObject2;
						BaseRAObject leftR = joinRA.leftChild;
						BaseRAObject rightR = joinRA.rightChild;
						System.out.println("Classs is "+leftR.getClass());
						RelationRAObject leftrel = null,rightrel=null; 
						if(leftR instanceof RelationRAObject){
							leftrel = (RelationRAObject) leftR;
							System.out.println("Left table from tree is "+leftrel.tablename);
						}
						else {
							continue;
						}
						if(rightR instanceof RelationRAObject){
							rightrel = (RelationRAObject) rightR;
							System.out.println("Right table from tree is "+rightrel.tablename);							
						}
						else {
							continue;
						}
						HybridHashRAObject hr = new HybridHashRAObject();
						hr.operator = RAOperator.HYBRID_HASH;		
						
						for (Expression expression : equal_op) {
							System.out.println("Exp:"+expression);
							EqualsTo equalsTo = (EqualsTo) expression;
							Column left = (Column) equalsTo.getLeftExpression();
							Column right = (Column) equalsTo.getRightExpression();
							Table leftT = left.getTable();							
							Table rightT = right.getTable();																										
							System.out.println("Left table is "+leftT);
							System.out.println("Right table is "+rightT);
							int j=0;
							String joiningattributes[]=new String[2];
							System.out.println("Left column is "+left);
							System.out.println("Right column is "+right);
							if((leftT.getName().equalsIgnoreCase(leftrel.tablename))  && (rightT.getName().equalsIgnoreCase(rightrel.tablename)))
							{								
								System.out.println("Removed expression is "+expression);
								refactor_copy.remove(i);
								hr.leftChild = leftR;
								hr.rightChild =rightR;
								
								joiningattributes[0]=left.getWholeColumnName().toString();
								joiningattributes[1]=right.getWholeColumnName().toString();
								hr.joiningattributes=joiningattributes;
								refactor_copy.add(i,hr);
								exp_to_be_removed.add(expression);
							}
							else if((leftT.getName().equalsIgnoreCase(rightrel.tablename))  && (rightT.getName().equalsIgnoreCase(leftrel.tablename)))
							{						
								System.out.println("Removed expression is "+expression);
								refactor_copy.remove(i);
								hr.leftChild = leftR;
								hr.rightChild =rightR;

								joiningattributes[0]=left.toString();
								joiningattributes[1]=right.toString();
								hr.joiningattributes=joiningattributes;
								
								refactor_copy.add(i,hr);
								exp_to_be_removed.add(expression);
							}							
						}					
					}
					i++;
				}
				
				refactor = (Stack<BaseRAObject>) refactor_copy.clone();
				
				//Remove the equality expression from the refactor_copy by iterating over the RA
				for (Expression expression : exp_to_be_removed) {
					i=0;
					for (BaseRAObject baseRAObject2 : refactor_copy) {
						if(baseRAObject2.operator.compareTo(RAOperator.SELECT)==0)
						{
							WhereRAObject sel = (WhereRAObject)baseRAObject2;
							Expression pass = sel.exp;
							System.out.println("Expression is "+pass);
							Expression exp_data;
							
							if(pass instanceof Parenthesis)
							{
								Parenthesis parathesis= (Parenthesis) pass;
								exp_data = parathesis.getExpression();
							}
							else
							{
								exp_data = pass;
							}
						
							System.out.println("Expression inside is "+exp_data);
							System.out.println("The expression type is "+pass.getClass());
							if(exp_data.equals(expression))
								{
								
									System.out.println("Removing form refactor");
									System.out.println("Removed expression is "+expression);
									System.out.println("Rmoving fro refactor "+refactor.get(i).operator);
									System.out.println(i);									
									refactor.remove(i);
								}												
						
					}
						i++;
				}
				}
				
				System.out.println("After removing all the joins");
				for (BaseRAObject baseRAObject2 : refactor) {
					System.out.println(baseRAObject2.operator);
				}
				
				
				while(!refactor.isEmpty()){
					BaseRAObject operation = refactor.pop();
					if(operation.operator.compareTo(RAOperator.PRINT)==0)						
					{
						System.out.println("Print operator called");
						//ExProjectRAObject extPRA = (ExProjectRAObject)operation;
						PrintRAObject prntRA = (PrintRAObject)operation;
						PrintRAOperator printOpr = new PrintRAOperator(prevOp);
						prevOp=printOpr;
						System.out.println("Print i");
						printOpr.getNext();
						//tuple = 
						
					}
					else if(operation.operator.compareTo(RAOperator.RELATION)==0){
						RelationRAObject rel = (RelationRAObject)operation;
						System.out.println(rel.tablename);
						System.out.println("Relation tablename is "+relations.get(rel.tablename));
						FileReaderOperator tableOpr = new FileReaderOperator(relations.get(rel.tablename));
						schema = relations.get(rel.tablename);
						prevOp = tableOpr;
						//tuple = tableOpr.getNext();
					}
					else if(operation.operator.compareTo(RAOperator.UNION)==0){
						UnionRAObject unions = (UnionRAObject)operation;
						this.processRelationalAlgebra(unions.statementHeads);
					}
					else if(operation.operator.compareTo(RAOperator.SELECT)==0){
						WhereRAObject sel = (WhereRAObject)operation;
						SelectOperator select = new SelectOperator(prevOp, sel.exp, schema);
						//tuple = select.getNext();
						prevOp = select;
					}
					else if(operation.operator.compareTo(RAOperator.ORDER_BY)==0){
						OrderByRAObject orByRA = (OrderByRAObject) operation;
						OrderbyRAOperator orderbyRAOperator = new OrderbyRAOperator(prevOp,schema,orByRA.orderByElements,orByRA.items);
						prevOp = orderbyRAOperator;						
					}
					else if(operation.operator.compareTo(RAOperator.GROUP_BY)==0){
						GroupByRAObject gpByRA = (GroupByRAObject) operation;
						GroupByOperator gpByOpr = new GroupByOperator(prevOp, schema, gpByRA.columnReferences,gpByRA.items);
						System.out.println("Group by elements are "+gpByRA.columnReferences);
						prevOp = gpByOpr;
					}
					else if(operation.operator.compareTo(RAOperator.EXTENDED_PROJECT)==0){
						ExProjectRAObject extPRA = (ExProjectRAObject)operation;
						ExtProjOperator extProj = new ExtProjOperator(prevOp, extPRA.items,schema,extPRA.GroupbyFound);
						System.out.println("Extended project called");
						prevOp = extProj;						
					}
					else if(operation.operator.compareTo(RAOperator.JOIN)==0){
						JoinRAObject joinRA = (JoinRAObject) operation;
						RelationRAObject leftR =  (RelationRAObject)joinRA.leftChild;
						RelationRAObject rightR =  (RelationRAObject)joinRA.rightChild;
						/*FileReaderOperator leftTableOpr = new FileReaderOperator(relations.get(leftR.tablename));
						SchemaBean leftSchema = relations.get(leftR.tablename);
						FileReaderOperator rightTableOpr = new FileReaderOperator(relations.get(rightR.tablename));
						SchemaBean rightSchema = relations.get(rightR.tablename);*/
						//joinRA.leftChild.operator.
						Operator leftTableOpr; 						
						if((leftR instanceof JoinRAObject)||(leftR instanceof HybridHashRAObject))
							leftTableOpr = ((JoinRAObject)leftR).ListOperator;
						else
							leftTableOpr = new FileReaderOperator(relations.get(leftR.tablename));
						SchemaBean leftSchema = relations.get(leftR.tablename);
						
						System.out.println("File name is "+relations.get(leftR.tablename));
						System.out.println("Left tablename is "+leftR.tablename);
						System.exit(0);
						Operator rightTableOpr; 
						if((rightR instanceof JoinRAObject)||(rightR instanceof HybridHashRAObject))
							rightTableOpr = ((JoinRAObject)rightR).ListOperator;
						else
							rightTableOpr = new FileReaderOperator(relations.get(rightR.tablename));
						SchemaBean rightSchema = relations.get(rightR.tablename);
						System.out.println("Left schema is "+leftSchema.file.getName());
						System.exit(0);
						
						XProductOperator xPOpr = new XProductOperator(leftTableOpr, rightTableOpr, leftSchema, rightSchema, schema);
						List<ColumnDefinition> newColNames = new ArrayList<ColumnDefinition>();
						newColNames.addAll(leftSchema.colNames);
						newColNames.addAll(rightSchema.colNames);
						//schema = new SchemaBean(GlobalConstants.JOIN_FILE, newColNames);
						//schema = new SchemaBean(leftSchema.file.getName(), rightSchema.file.getName(),
						//		leftSchema.colNames,rightSchema.colNames,newColNames);
						schema = new SchemaBean(leftSchema,rightSchema);
						//System.out.println(schema.file.getName());
						joinRA.tablename = schema.file.getName();
						relations.put(schema.file.getName(), schema);
						if(joinRA.onExp==null){
							prevOp = xPOpr;
							joinRA.ListOperator = xPOpr;
						}else{
							SelectOperator selJoin = new SelectOperator(xPOpr, joinRA.onExp, schema);
							prevOp = selJoin;
							joinRA.ListOperator = selJoin;
						}
					}
					else if(operation.operator.compareTo(RAOperator.HYBRID_HASH)==0){
						//Make an RA 
						//HybridHashRAObject hr = new HybridHashRAObject();
						System.out.println("Reaching hybridhash");
						//System.exit(0);
						HybridHashRAObject joinRA = (HybridHashRAObject) operation;
						RelationRAObject leftR =  (RelationRAObject)joinRA.leftChild;
						RelationRAObject rightR =  (RelationRAObject)joinRA.rightChild;
						
						/*FileReaderOperator leftTableOpr = new FileReaderOperator(relations.get(leftR.tablename));
						SchemaBean leftSchema = relations.get(leftR.tablename);
						FileReaderOperator rightTableOpr = new FileReaderOperator(relations.get(rightR.tablename));
						SchemaBean rightSchema = relations.get(rightR.tablename);*/
						
						//If it is hybrid hash join both left and right table is a relation
						
						SchemaBean rightSchema = relations.get(rightR.tablename);
						SchemaBean leftSchema = relations.get(leftR.tablename);
						System.out.println("Right table is "+rightR.tablename);

						String joiningAttributes[]=joinRA.joiningattributes;
						System.out.println("Joining attributes are "+joiningAttributes[0]+" "+joiningAttributes[1]);
						//System.out.println("Filename is "+rightSchema.file.getAbsolutePath());
						File swapDirectory = new File(GlobalConstants.SWAP_DATA_PATH);						
						System.out.println("File is "+rightSchema.file.getName());
						FileReaderOperator rightTableOpr = new FileReaderOperator(relations.get(rightR.tablename));
						FileReaderOperator leftTableOpr = new FileReaderOperator(relations.get(leftR.tablename));
						System.out.println("Left table name is "+leftR.tablename);

						HybridHashJoinOperator hashJoinOperator = new HybridHashJoinOperator(leftTableOpr, leftSchema, rightTableOpr, rightSchema, joiningAttributes, swapDirectory);
 
						/*if(leftR instanceof JoinRAObject)
							leftTableOpr = ((JoinRAObject)leftR).ListOperator;
						else
							leftTableOpr = new FileReaderOperator(relations.get(leftR.tablename));
						SchemaBean leftSchema = relations.get(leftR.tablename);
						
						Operator rightTableOpr; 
						if(rightR instanceof JoinRAObject)
							rightTableOpr = ((JoinRAObject)rightR).ListOperator;
						else
							rightTableOpr = new FileReaderOperator(relations.get(rightR.tablename));*/
						
						
						
						/*XProductOperator xPOpr = new XProductOperator(leftTableOpr, rightTableOpr, leftSchema, rightSchema, schema);
						List<ColumnDefinition> newColNames = new ArrayList<ColumnDefinition>();
						newColNames.addAll(leftSchema.colNames);
						newColNames.addAll(rightSchema.colNames);*/
						//schema = new SchemaBean(GlobalConstants.JOIN_FILE, newColNames);
						//schema = new SchemaBean(leftSchema.file.getName(), rightSchema.file.getName(),
						//		leftSchema.colNames,rightSchema.colNames,newColNames);
						//schema = new SchemaBean(leftSchema,rightSchema);
						//System.out.println(schema.file.getName());
						/*joinRA.tablename = schema.file.getName();
						relations.put(schema.file.getName(), schema);
						if(joinRA.onExp==null){
							prevOp = xPOpr;
							joinRA.ListOperator = xPOpr;
						}else{
							SelectOperator selJoin = new SelectOperator(xPOpr, joinRA.onExp, schema);
							prevOp = selJoin;
							joinRA.ListOperator = selJoin;
						}*/
						
						schema = new SchemaBean(leftSchema,rightSchema);
						joinRA.tablename = schema.file.getName();
						relations.put(schema.file.getName(), schema);
						
						
						
					}
					else if(operation.operator.compareTo(RAOperator.BLOCK_NESTED)==0){
						JoinRAObject joinRA = (JoinRAObject) operation;
						RelationRAObject leftR =  (RelationRAObject)joinRA.leftChild;
						RelationRAObject rightR =  (RelationRAObject)joinRA.rightChild;
						/*FileReaderOperator leftTableOpr = new FileReaderOperator(relations.get(leftR.tablename));
						SchemaBean leftSchema = relations.get(leftR.tablename);
						FileReaderOperator rightTableOpr = new FileReaderOperator(relations.get(rightR.tablename));
						SchemaBean rightSchema = relations.get(rightR.tablename);*/
						Operator leftTableOpr; 
						if(leftR instanceof JoinRAObject)
							leftTableOpr = ((JoinRAObject)leftR).ListOperator;
						else
							leftTableOpr = new FileReaderOperator(relations.get(leftR.tablename));
						SchemaBean leftSchema = relations.get(leftR.tablename);
						
						Operator rightTableOpr; 
						if(rightR instanceof JoinRAObject)
							rightTableOpr = ((JoinRAObject)rightR).ListOperator;
						else
							rightTableOpr = new FileReaderOperator(relations.get(rightR.tablename));
						SchemaBean rightSchema = relations.get(rightR.tablename);
						
						XProductOperator xPOpr = new XProductOperator(leftTableOpr, rightTableOpr, leftSchema, rightSchema, schema);
						List<ColumnDefinition> newColNames = new ArrayList<ColumnDefinition>();
						newColNames.addAll(leftSchema.colNames);
						newColNames.addAll(rightSchema.colNames);
						//schema = new SchemaBean(GlobalConstants.JOIN_FILE, newColNames);
						//schema = new SchemaBean(leftSchema.file.getName(), rightSchema.file.getName(),
						//		leftSchema.colNames,rightSchema.colNames,newColNames);
						schema = new SchemaBean(leftSchema,rightSchema);
						//System.out.println(schema.file.getName());
						joinRA.tablename = schema.file.getName();
						relations.put(schema.file.getName(), schema);
						if(joinRA.onExp==null){
							prevOp = xPOpr;
							joinRA.ListOperator = xPOpr;
						}else{
							SelectOperator selJoin = new SelectOperator(xPOpr, joinRA.onExp, schema);
							prevOp = selJoin;
							joinRA.ListOperator = selJoin;
						}
					}
				}
				
				
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void getStackFromRATree(Stack<BaseRAObject> operators,BaseRAObject node){
		if(node==null)
			{System.out.println("Returning");
			return;
			}
		System.out.println("Getting RA stack from tree");
		System.out.println("The operator being pushed is "+node.operator.name());
		operators.push(node);		
		getStackFromRATree(operators, node.rightChild);
		getStackFromRATree(operators, node.leftChild);
	}

}
