package testcases;

import static org.junit.Assert.*;

import org.junit.Test;

import SQLExpression.Expression;
import SQLParser.LogicalExpressionParser;

/**
 * This class handles the test cases of the having query language.
 * Note that for the format of the query, every element in the language
 * is seperated by a space except the aggregation which will be grouped
 * together. Notice I check the pre-order traverseal and the in order 
 * traversal of the tree to guarantee that we make the tree in the 
 * correct form.
 * @author messfish
 *
 */
public class LogicalExpressionTest {

	/** this case tests the sentence without parenthesis */
	@Test
	public void test1() {
		String exp = "COUNT(A) < 3.2 AND MIN(B) > 12.0 + C OR MAX(D) = 4";
		LogicalExpressionParser parse = 
				new LogicalExpressionParser(exp.split("\\s+"));
		Expression express = parse.parse();
		String expected = "[OR]" + "\n" +
		                  "-[AND]" + "\n" +
				          "--[<]" + "\n" +
		                  "---[Column: COUNT(A)]" + "\n" +
				          "---[Double: 3.2]" + "\n" +
		                  "--[>]" + "\n" +
				          "---[Column: MIN(B)]" + "\n" +
		                  "---[+]" + "\n" +
				          "----[Double: 12.0]" + "\n" +
		                  "----[Column: C]" + "\n" +
				          "-[=]" + "\n" +
		                  "--[Column: MAX(D)]" + "\n" +
				          "--[Long: 4]" + "\n";
		StringBuilder sb = new StringBuilder();
		express.print("", sb);
		/* test whether the tree structure is the same in pre-order from. */
		assertEquals(expected, sb.toString());
		StringBuilder sb2 = new StringBuilder();
		express.toString(sb2);
		sb2.deleteCharAt(sb2.length() - 1);
		/* test whether the tree structure is the same in in-order form. */
		assertEquals(exp, sb2.toString());
	}

	@Test
	/** This case tests the case with the parenthesis. */
	public void test2() {
		String exp = "MIN(A) <= 12 * ( 3.1 + 4.2 ) AND " +
					 "( MAX(B) >= 16.0 OR SUM(C) <> 12.0 / ( 6.0 - 4.0 ) )";
		LogicalExpressionParser parse = 
				new LogicalExpressionParser(exp.split("\\s+"));
		Expression express = parse.parse();
		String expected = "[AND]" +"\n" +
		                  "-[<=]" + "\n" +
				          "--[Column: MIN(A)]" + "\n" +
		                  "--[*]" +"\n" +
				          "---[Long: 12]" + "\n" +
		                  "---[()]" + "\n" +
		                  "----[+]" + "\n" +
				          "-----[Double: 3.1]" + "\n" +
		                  "-----[Double: 4.2]" + "\n" +
				          "-[()]" + "\n" +
		                  "--[OR]" + "\n" +
				          "---[>=]" + "\n" +
		                  "----[Column: MAX(B)]" + "\n" +
				          "----[Double: 16.0]" + "\n" +
		                  "---[<>]" + "\n" +
				          "----[Column: SUM(C)]" + "\n" +
		                  "----[/]" + "\n" +
				          "-----[Double: 12.0]" + "\n" +
		                  "-----[()]" + "\n" +
		                  "------[-]" + "\n" +
				          "-------[Double: 6.0]" + "\n" +
		                  "-------[Double: 4.0]" + "\n";
		StringBuilder sb = new StringBuilder();
		express.print("", sb);
		/* test whether the tree structure is the same in pre-order form. */
		assertEquals(expected, sb.toString());
		StringBuilder sb2 = new StringBuilder();
		express.toString(sb2);
		sb2.deleteCharAt(sb2.length() - 1);
		/* test whether the string is the same in in-order form. */
		assertEquals(exp, sb2.toString());
	}
	
	@Test
	/** this method tests the case when the not operator exists. */
	public void test3() {
		String exp = "NOT AVG(A) < 10.0 OR NOT " +
					 "( AVG(D) > 4.3 AND ( SUM(B) = 10.2 OR" +
				     " MIN(C) < 10.0 / ( 1.3 + 3.7 ) ) )" +
					 " AND SUM(E) < 9.0";
		LogicalExpressionParser parse = 
				new LogicalExpressionParser(exp.split("\\s+"));
		Expression express = parse.parse();
		String expected = "[AND]" + "\n" +
						  "-[OR]" + "\n" +
		                  "--[NOT]" + "\n" +
				          "---[<]" + "\n" +
		                  "----[Column: AVG(A)]" + "\n" +
				          "----[Double: 10.0]" + "\n" +
		                  "--[NOT]" + "\n" +
				          "---[()]" + "\n" +
		                  "----[AND]" + "\n" +
				          "-----[>]" + "\n" +
		                  "------[Column: AVG(D)]" + "\n" +
				          "------[Double: 4.3]" + "\n" +
		                  "-----[()]" + "\n" +
				          "------[OR]" + "\n" +
		                  "-------[=]" + "\n" +
				          "--------[Column: SUM(B)]" + "\n" +
		                  "--------[Double: 10.2]" + "\n" +
				          "-------[<]" + "\n" +
		                  "--------[Column: MIN(C)]" + "\n" +
				          "--------[/]" + "\n" +
		                  "---------[Double: 10.0]" + "\n" +
				          "---------[()]" + "\n" +
				          "----------[+]" + "\n" +
		                  "-----------[Double: 1.3]" + "\n" +
				          "-----------[Double: 3.7]" + "\n" +
		                  "-[<]" + "\n" + 
				          "--[Column: SUM(E)]" + "\n" +
		                  "--[Double: 9.0]" + "\n";
		StringBuilder sb = new StringBuilder();
		express.print("", sb);
		/* test whether the tree structure is the same in pre-order form. */
		assertEquals(expected, sb.toString());
		StringBuilder sb2 = new StringBuilder();
		express.toString(sb2);
		sb2.deleteCharAt(sb2.length() - 1);
		/* test whether the string is the same in in order form. */
		assertEquals(exp, sb2.toString());
	}
	
	/**
	 * This method tests the case when there is a like query.
	 */
	@Test
    public void test4() {
    	String exp = "S.A LIKE \"%%d%%\" AND S.B NOT LIKE \"%%%%\"";
    	LogicalExpressionParser parse = 
				new LogicalExpressionParser(exp.split("\\s+"));
		Expression express = parse.parse();
		String expected = "[AND]" + "\n" +
					  	  "-[LIKE]" + "\n" +
					  	  "--[Column: S.A]" + "\n" +
					  	  "--[String: \"%%d%%\"]" + "\n" +
					  	  "-[NOT]" + "\n" +
					  	  "--[LIKE]" + "\n" +
					  	  "---[Column: S.B]" + "\n" +
					  	  "---[String: \"%%%%\"]" + "\n";
		StringBuilder sb = new StringBuilder();
		express.print("", sb);
		/* test whether the tree structure is the same in pre-order form. */
		assertEquals(expected, sb.toString());
    }
    
}

