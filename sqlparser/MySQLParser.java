package edu.buffalo.cse562.sqlparser;

import java.io.StringReader;
import java.util.List;

import edu.buffalo.cse562.beans.SchemaBean;
import edu.buffalo.cse562.raobjects.BaseRAObject;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class MySQLParser
{
    CCJSqlParserManager parserManager = new CCJSqlParserManager();

    public MySQLParser() throws JSQLParserException
    {
        String statement = "SELECT COUNT(*),A.X FROM db.table1,X,Y,Z where x.a='5' and y.b = 7 group by name";
        SelectParser parser = new SelectParser();
        BaseRAObject head = parser.createRAFromSelect(((Select) parserManager.parse(new StringReader(statement))));
        traverse(head);
        PlainSelect plainSelect = (PlainSelect) ((Select) parserManager.parse(new StringReader(statement))).getSelectBody();        
        System.out.format("%s is function call? %s",
                plainSelect.getSelectItems().get(0),
                ((Function)((SelectExpressionItem) plainSelect.getSelectItems().get(0)).getExpression()).isAllColumns());
        List<Column> groupByCols = plainSelect.getGroupByColumnReferences();
        
        String ctStatement = "CREATE TABLE R(A Varchar, B int, C int);";
        CreateTable ctstatement1 = (CreateTable) parserManager.parse(new StringReader(ctStatement));
        CreateTableParser parser1 = new CreateTableParser();
        SchemaBean bean = parser1.parseCreateTable(ctstatement1);
        System.out.println(bean.file.getAbsolutePath());
        for(ColumnDefinition str:bean.colNames){
        	System.out.println(str.getColumnName());
        	System.out.println(bean.colIdx.get(str.getColumnName()));
        	System.out.println(str.getColDataType());
        }
        
        
    }
    public static void traverse(BaseRAObject obj){
    	if(obj==null)
    		return;
    	System.out.println(obj.operator);
    	traverse(obj.leftChild);
    	traverse(obj.rightChild);
    }
    public static void main(String[] args) throws JSQLParserException
    {

        new MySQLParser();

    }
    
}