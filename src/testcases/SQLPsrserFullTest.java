package testcases;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import SQLExpression.AddOperator;
import SQLExpression.AndOperator;
import SQLExpression.AnyOperator;
import SQLExpression.ColumnNode;
import SQLExpression.DateValue;
import SQLExpression.DivideOperator;
import SQLExpression.DoubleValue;
import SQLExpression.Expression;
import SQLExpression.GreaterThan;
import SQLExpression.GreaterThanOrEquals;
import SQLExpression.InOperator;
import SQLExpression.LessThan;
import SQLExpression.LikeOperator;
import SQLExpression.LongValue;
import SQLExpression.MinusOperator;
import SQLExpression.MultiplyOperator;
import SQLExpression.NotOperator;
import SQLExpression.OrOperator;
import SQLExpression.Parenthesis;
import SQLExpression.StringValue;
import SQLExpression.Subselect;
import SQLParser.CheckEquals;
import SQLParser.PlainSelect;
import TableElement.Table;

/**
 * this class serves as the full test of the SQL parser. In general
 * it will test all kinds of possible combination of the basic SQL
 * query. I will create a plain select query and fetch all the 
 * associated elements to check whether they matches the expectations.
 * Notice the input could only be a valid query. That means it should 
 * follow the format defined in the PlainSelect class.
 * @author messfish
 *
 */
public class SQLPsrserFullTest {

	/**
	 * this method tests the most fundamental query.
	 */
	@Test
	public void testBasic() {
		String query = "SELECT * FROM Sailors";
		PlainSelect plain = new PlainSelect(query);
		CheckEquals check = new CheckEquals();
		assertTrue(check.isEqual(plain.getSelectElements(), new ArrayList<>()));
		assertFalse(plain.isDistinct());
		Map<String, Table> tablemap = new HashMap<>();
		tablemap.put("Sailors", new Table("Sailors"));
		assertTrue(check.checkEqual(plain.getFromList(), tablemap));
		assertTrue(check.checkEqual(null, plain.getWhereExpression()));
		assertTrue(check.checkEqual(plain.getGroupByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(null, plain.getHavingExpression()));
		assertTrue(check.isEqual(plain.getOrderByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(plain.isDescList(), null));
	}
	
	/**
	 * this method tests the query that contains a where query.
	 */
	@Test
	public void testWhere() {
		String query = "SELECT Sailors.A FROM Sailors WHERE Sailors.B > 9000";
		PlainSelect plain = new PlainSelect(query);
		CheckEquals check = new CheckEquals();
		List<Expression> list = new ArrayList<>();
		list.add(new ColumnNode("Sailors.A"));
		assertTrue(check.isEqual(plain.getSelectElements(), list));
		assertFalse(plain.isDistinct());
		List<String> aliaslist = new ArrayList<>();
		aliaslist.add("");
		assertTrue(check.checkEqual(plain.getSelectAlias(), aliaslist));
		Map<String, Table> tablemap = new HashMap<>();
		tablemap.put("Sailors", new Table("Sailors"));
		assertTrue(check.checkEqual(plain.getFromList(), tablemap));
		Expression exp1 = new ColumnNode("Sailors.B");
		Expression exp2 = new LongValue(9000);
		Expression exp3 = new GreaterThan(exp1, exp2);
		assertTrue(check.checkEqual(plain.getWhereExpression(), exp3));
		assertTrue(check.checkEqual(plain.getGroupByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(null, plain.getHavingExpression()));
		assertTrue(check.isEqual(plain.getOrderByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(plain.isDescList(), null));
	}
	
	/**
	 * this method tests the query that contains a group by query.
	 */
	@Test
	public void testGroupBy() {
		String query = "SELECT Sailors.A AS SB FROM Sailors GROUP BY Sailors.A";
		PlainSelect plain = new PlainSelect(query);
		CheckEquals check = new CheckEquals();
		List<Expression> list = new ArrayList<>();
		list.add(new ColumnNode("Sailors.A"));
		assertTrue(check.isEqual(plain.getSelectElements(), list));
		assertFalse(plain.isDistinct());
		List<String> aliaslist = new ArrayList<>();
		aliaslist.add("SB");
		assertTrue(check.checkEqual(plain.getSelectAlias(), aliaslist));
		Map<String, Table> tablemap = new HashMap<>();
		tablemap.put("Sailors", new Table("Sailors"));
		assertTrue(check.checkEqual(plain.getFromList(), tablemap));
		assertTrue(check.checkEqual(null, plain.getWhereExpression()));
		List<String> list1 = new ArrayList<>();
		list1.add("Sailors.A");
		assertTrue(check.checkEqual(plain.getGroupByElements(), list1));
		assertTrue(check.checkEqual(null, plain.getHavingExpression()));
		assertTrue(check.isEqual(plain.getOrderByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(plain.isDescList(), null));
	}
	
	/**
	 * this method tests the query that contains a having query.
	 */
	@Test
	public void testHaving() {
		String query = "SELECT S.A , COUNT ( DISTINCT * ) "
				+ "FROM Sailors AS S GROUP BY S.A "
				+ "HAVING MIN ( S.A ) >= 10.0 AND 100 < MAX ( S.A )";
		PlainSelect plain = new PlainSelect(query);
		CheckEquals check = new CheckEquals();
		List<Expression> list = new ArrayList<>();
		list.add(new ColumnNode("S.A"));
		list.add(new ColumnNode("COUNT(DISTINCT*)"));
		assertTrue(check.isEqual(plain.getSelectElements(), list));
		assertFalse(plain.isDistinct());
		List<String> aliaslist = new ArrayList<>();
		aliaslist.add("");
		aliaslist.add("");
		assertTrue(check.checkEqual(plain.getSelectAlias(), aliaslist));
		Map<String, Table> tablemap = new HashMap<>();
		tablemap.put("S", new Table("Sailors"));
		assertTrue(check.checkEqual(plain.getFromList(), tablemap));
		assertTrue(check.checkEqual(null, plain.getWhereExpression()));
		List<String> list1 = new ArrayList<>();
		list1.add("S.A");
		assertTrue(check.checkEqual(plain.getGroupByElements(), list1));
		Expression exp1 = new ColumnNode("MIN(S.A)");
		Expression exp2 = new DoubleValue(10.0);
		Expression exp3 = new LongValue(100);
		Expression exp4 = new ColumnNode("MAX(S.A)");
		Expression exp5 = new GreaterThanOrEquals(exp1, exp2);
		Expression exp6 = new LessThan(exp3, exp4);
		Expression exp7 = new AndOperator(exp5, exp6);
		assertTrue(check.checkEqual(plain.getHavingExpression(), exp7));
		assertTrue(check.isEqual(plain.getOrderByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(plain.isDescList(), null));
	}
	
	/**
	 * this method that tests the query that contains the order by clause.
	 */
	@Test
	public void testOrderBy() {
		String query = "SELECT S.A * S.B AS SB "
				+ "FROM Sailors AS S "
				+ "ORDER BY S.A * ( S.C + 100 )"; 
		PlainSelect plain = new PlainSelect(query);
		CheckEquals check = new CheckEquals();
		List<Expression> list = new ArrayList<>(); 
		Expression exp1 = new ColumnNode("S.A");
		Expression exp2 = new ColumnNode("S.B");
		Expression exp3 = new MultiplyOperator(exp1, exp2);
		list.add(exp3);
		assertTrue(check.isEqual(plain.getSelectElements(), list));
		assertFalse(plain.isDistinct());
		List<String> list1 = new ArrayList<>();
		list1.add("SB");
		assertTrue(check.checkEqual(plain.getSelectAlias(), list1));
		Map<String, Table> tablemap = new HashMap<>();
		tablemap.put("S", new Table("Sailors"));
		assertTrue(check.checkEqual(plain.getFromList(), tablemap));
		assertTrue(check.checkEqual(null, plain.getWhereExpression()));
		assertTrue(check.checkEqual(plain.getGroupByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(null, plain.getHavingExpression()));
		Expression exp4 = new ColumnNode("S.C");
		Expression exp5 = new LongValue(100);
		Expression exp6 = new AddOperator(exp4, exp5);
		Expression exp7 = new Parenthesis(exp6);
		Expression exp8 = new ColumnNode("S.A");
		Expression exp9 = new MultiplyOperator(exp8, exp7);
		List<Expression> list2 = new ArrayList<>();
		list2.add(exp9);
		assertTrue(check.isEqual(plain.getOrderByElements(), list2));
		assertTrue(check.checkEqual(plain.isDescList(), new int[]{1}));
		assertEquals(0, plain.getStartPoint());
		assertEquals(0, plain.getEndPoint());
	}
	
	/**
	 * This method is used to test complex query by having multiple
	 * elements in the query.
	 */
	@Test
	public void testCombinations1() {
		String query = "SELECT S.A , S.B , B.E "
				+ "FROM Boats AS B , Sailors AS S , Reserves "
				+ "WHERE Reserves.G LIKE \"%%_%%\" AND NOT S.A >= "
				+ "3.4 * ( 10 + 6.0 / B.D ) "
				+ "ORDER BY S.C DESC";
		PlainSelect plain = new PlainSelect(query);
		CheckEquals check = new CheckEquals();
		List<Expression> list = new ArrayList<>();
		list.add(new ColumnNode("S.A"));
		list.add(new ColumnNode("S.B"));
		list.add(new ColumnNode("B.E"));
		assertTrue(check.isEqual(plain.getSelectElements(), list));
		assertFalse(plain.isDistinct());
		List<String> list1 = new ArrayList<>();
		list1.add("");
		list1.add("");
		list1.add("");
		assertTrue(check.checkEqual(plain.getSelectAlias(), list1));
		Map<String, Table> tablemap = new HashMap<>();
		tablemap.put("S", new Table("Sailors"));
		tablemap.put("B", new Table("Boats"));
		tablemap.put("Reserves", new Table("Reserves"));
		assertTrue(check.checkEqual(plain.getFromList(), tablemap));
		Expression exp1 = new ColumnNode("Reserves.G");
		Expression exp2 = new StringValue("\"%%_%%\"");
		Expression exp3 = new LikeOperator(exp1, exp2);
		Expression exp4 = new ColumnNode("S.A");
		Expression exp5 = new LongValue(10);
		Expression exp6 = new DoubleValue(6.0);
		Expression exp7 = new ColumnNode("B.D");
		Expression exp8 = new DivideOperator(exp6, exp7);
		Expression exp9 = new AddOperator(exp5, exp8);
		Expression exp10 = new Parenthesis(exp9);
		Expression exp11 = new DoubleValue(3.4);
		Expression exp12 = new MultiplyOperator(exp11, exp10);
		Expression exp13 = new GreaterThanOrEquals(exp4, exp12);
		Expression exp14 = new NotOperator(exp13);
		Expression exp15 = new AndOperator(exp3, exp14);
		assertTrue(check.checkEqual(plain.getWhereExpression(), exp15));
		assertTrue(check.checkEqual(plain.getGroupByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(null, plain.getHavingExpression()));
		List<Expression> list2 = new ArrayList<>();
		list2.add(new ColumnNode("S.C"));
		assertTrue(check.isEqual(plain.getOrderByElements(), list2));
		assertTrue(check.checkEqual(plain.isDescList(), new int[]{-1}));
		assertEquals(0, plain.getStartPoint());
		assertEquals(0, plain.getEndPoint());
	}
	
	/**
	 * This method is used to test complex query by having a sub query
	 * in the query.
	 */
	@Test
	public void testCombinations2() {
		String query = "SELECT DISTINCT Mule.A , R.H "
					   + "FROM ( SELECT S.A AS SB "
					   + "FROM Sailors AS S , Boats "
					   + "WHERE Boats.E = S.C ) AS Mule , Reserves AS R "
					   + "GROUP BY Mule.A , R.H "
					   + "ORDER BY R.H LIMIT 10 , 60";
		PlainSelect plain = new PlainSelect(query);
		CheckEquals check = new CheckEquals();
		List<Expression> list = new ArrayList<>();
		list.add(new ColumnNode("Mule.A"));
		list.add(new ColumnNode("R.H"));
		assertTrue(check.isEqual(plain.getSelectElements(), list));
		List<String> aliaslist = new ArrayList<>();
		aliaslist.add("");
		aliaslist.add("");
		assertTrue(check.checkEqual(plain.getSelectAlias(), aliaslist));
		assertTrue(plain.isDistinct());
		String sub = "SELECT S.A AS SB "
				   + "FROM Sailors AS S , Boats "
				   + "WHERE Boats.E = S.C";
		Map<String, Table> tablemap = new HashMap<>();
		tablemap.put("Mule", new Table("Mule", new PlainSelect(sub)));
		tablemap.put("R", new Table("Reserves"));
		assertTrue(check.checkEqual(plain.getFromList(), tablemap));
		assertTrue(check.checkEqual(plain.getWhereExpression(), null));
		List<String> list1 = new ArrayList<>();
		list1.add("Mule.A");
		list1.add("R.H");
		assertTrue(check.checkEqual(plain.getGroupByElements(), list1));
		List<Expression> list2 = new ArrayList<>();
		list2.add(new ColumnNode("R.H"));
		assertTrue(check.isEqual(plain.getOrderByElements(), list2));
		assertTrue(check.checkEqual(plain.isDescList(), new int[]{1}));
		assertEquals(10, plain.getStartPoint());
		assertEquals(60, plain.getEndPoint());
	}

	/**
	 * This method is used to test complex query by having a sub query
	 * in the query.
	 */
	@Test
	public void testComboinations3() {
		String query = "SELECT DISTINCT * "
				+ "FROM Sailors AS S "
				+ "WHERE S.B NOT IN ( SELECT Boats.F "
				+ "FROM Boats "
				+ "GROUP BY Boats.E , Boats.F ) OR S.C < \"1998/12/30\" "
				+ "ORDER BY S.C , S.B DESC LIMIT 12 , 16";
		PlainSelect plain = new PlainSelect(query);
		CheckEquals check = new CheckEquals();
		assertTrue(check.isEqual(plain.getSelectElements(), new ArrayList<>()));
		assertTrue(plain.isDistinct());
		Map<String, Table> tablemap = new HashMap<>();
		tablemap.put("S", new Table("Sailors"));
		assertTrue(check.checkEqual(plain.getFromList(), tablemap));
		String sub = "SELECT Boats.F "
				+ "FROM Boats "
				+ "GROUP BY Boats.E , Boats.F";
		Expression exp1 = new ColumnNode("S.B");
		Expression exp2 = new Parenthesis(new Subselect(sub));
		Expression exp3 = new InOperator(exp1, exp2);
		Expression exp4 = new NotOperator(exp3);
		Expression exp5 = new ColumnNode("S.C");
		Expression exp6 = new DateValue("\"1998/12/30\"");
		Expression exp7 = new LessThan(exp5,exp6);
		Expression exp8 = new OrOperator(exp4,exp7);
		assertTrue(check.checkEqual(plain.getWhereExpression(), exp8));
		assertTrue(check.checkEqual(plain.getGroupByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(plain.getHavingExpression(), null));
		List<Expression> list = new ArrayList<>();
		list.add(new ColumnNode("S.C"));
		list.add(new ColumnNode("S.B"));
		assertTrue(check.isEqual(plain.getOrderByElements(), list));
		assertTrue(check.checkEqual(plain.isDescList(), new int[]{1,-1}));
		assertEquals(12, plain.getStartPoint());
		assertEquals(16, plain.getEndPoint());
	}
	
	/**
	 * This method is used to test complex query by having a union query
	 * in the query.
	 */
	@Test
	public void testCombinations4() {
		String query = "( SELECT DISTINCT S.A * ( S.B + S.C ) AS SB , B.D AS BD "
				+ "FROM Sailors AS S , Boats AS B "
				+ "WHERE B.E < ANY ( SELECT Reserves.G "
				+ "FROM Reserves ) ) "
				+ "UNION ALL "
				+ "( SELECT Sailors.B AS SB , Boats.F AS BD "
				+ "FROM Sailors , Boats ) "
				+ "ORDER BY SB DESC , BD LIMIT 1 , 30";
		PlainSelect plain = new PlainSelect(query);
		assertEquals(2, plain.getUnionType());
		PlainSelect left = plain.getSubQueries()[0],
					right = plain.getSubQueries()[1];
		CheckEquals check = new CheckEquals();
		Expression exp1 = new ColumnNode("S.A");
		Expression exp2 = new ColumnNode("S.B");
		Expression exp3 = new ColumnNode("S.C");
		Expression exp4 = new AddOperator(exp2, exp3);
		Expression exp5 = new Parenthesis(exp4);
		Expression exp6 = new MultiplyOperator(exp1, exp5);
		List<Expression> list = new ArrayList<>();
		list.add(exp6);
		list.add(new ColumnNode("B.D"));
		assertTrue(check.isEqual(left.getSelectElements(), list));
		assertTrue(left.isDistinct());
		List<String> list1 = new ArrayList<>();
		list1.add("SB");
		list1.add("BD");
		assertTrue(check.checkEqual(left.getSelectAlias(), list1));
		Map<String, Table> tablemap = new HashMap<>();
		tablemap.put("S", new Table("Sailors"));
		tablemap.put("B", new Table("Boats"));
		assertTrue(check.checkEqual(left.getFromList(), tablemap));
		Expression exp7 = new ColumnNode("B.E");
		Expression exp8 = new Subselect("SELECT Reserves.G FROM Reserves");
		Expression exp9 = new Parenthesis(exp8);
		Expression exp10 = new AnyOperator(exp9);
		Expression exp11 = new LessThan(exp7, exp10);
		assertTrue(check.checkEqual(left.getWhereExpression(), exp11));
		assertTrue(check.checkEqual(left.getGroupByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(left.getHavingExpression(), null));
		List<Expression> list2 = new ArrayList<>();
		list2.add(new ColumnNode("Sailors.B"));
		list2.add(new ColumnNode("Boats.F"));
		assertTrue(check.isEqual(right.getSelectElements(), list2));
		assertTrue(check.checkEqual(right.getSelectAlias(), list1));
		Map<String, Table> tablemap1 = new HashMap<>();
		tablemap1.put("Sailors", new Table("Sailors"));
		tablemap1.put("Boats", new Table("Boats"));
		assertTrue(check.checkEqual(right.getFromList(), tablemap1));
		assertTrue(check.checkEqual(right.getWhereExpression(), null));
		assertTrue(check.checkEqual(right.getGroupByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(right.getHavingExpression(), null));
		List<Expression> list3 = new ArrayList<>();
		list3.add(new ColumnNode("SB"));
		list3.add(new ColumnNode("BD"));
		assertTrue(check.isEqual(plain.getOrderByElements(), list3));
		assertTrue(check.checkEqual(plain.isDescList(), new int[]{-1,1}));
		assertEquals(1, plain.getStartPoint());
		assertEquals(30, plain.getEndPoint());
	}
	
	/**
	 * This method is used to test complex query by having a union query
	 * as a sub query in the query.
	 */
	@Test
	public void testCombinations5() {
		String query = "SELECT S.A , COUNT ( S.B ) AS SB "
				+ "FROM ( ( SELECT Boats.B "
				+ "FROM Boats "
				+ "WHERE Boats.A >= 10.0 ) "
				+ "UNION "
				+ "( SELECT Boats.B "
				+ "FROM Boats "
				+ "WHERE Boats.C < 1000 ) ) AS B , Sailors AS S "
				+ "GROUP BY S.A "
				+ "HAVING MIN ( S.C ) < 100 * ( S.A - S.B )";
		PlainSelect plain = new PlainSelect(query);
		assertEquals(0, plain.getUnionType());
		CheckEquals check = new CheckEquals();
		List<Expression> list = new ArrayList<>();
		list.add(new ColumnNode("S.A"));
		list.add(new ColumnNode("COUNT(S.B)"));
		assertTrue(check.isEqual(list, plain.getSelectElements()));
		List<String> list1 = new ArrayList<>();
		list1.add("");
		list1.add("SB");
		assertTrue(check.checkEqual(list1, plain.getSelectAlias()));
		String subquery = "( SELECT Boats.B "
				+ "FROM Boats "
				+ "WHERE Boats.A >= 10.0 ) "
				+ "UNION "
				+ "( SELECT Boats.B "
				+ "FROM Boats "
				+ "WHERE Boats.C < 1000 )";
		PlainSelect subplain = new PlainSelect(subquery);
		assertEquals(1, subplain.getUnionType());
		Map<String, Table> map = new HashMap<>();
		map.put("B", new Table("B", subplain));
		map.put("S", new Table("Sailors"));
		assertTrue(check.checkEqual(plain.getFromList(), map));
		assertTrue(check.checkEqual(plain.getWhereExpression(), null));
		List<String> list2 = new ArrayList<>();
		list2.add("S.A");
		assertTrue(check.checkEqual(list2, plain.getGroupByElements()));
		Expression e1 = new ColumnNode("MIN(S.C)");
		Expression e2 = new LongValue(100);
		Expression e3 = new ColumnNode("S.A");
		Expression e4 = new ColumnNode("S.B");
		Expression e5 = new MinusOperator(e3,e4);
		Expression e8 = new Parenthesis(e5);
		Expression e6 = new MultiplyOperator(e2,e8);
		Expression e7 = new LessThan(e1,e6);
		assertTrue(check.checkEqual(plain.getHavingExpression(), e7));
		assertTrue(check.isEqual(plain.getOrderByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(plain.isDescList(), null));
		PlainSelect left = subplain.getSubQueries()[0],
				right = subplain.getSubQueries()[1];
		List<Expression> list3 = new ArrayList<>();
		list3.add(new ColumnNode("Boats.B"));
		Map<String, Table> tablemap = new HashMap<>();
		tablemap.put("Boats", new Table("Boats"));
		List<String> list4 = new ArrayList<>();
		list4.add("");
		assertTrue(check.isEqual(left.getSelectElements(), list3));
		assertTrue(check.isEqual(right.getSelectElements(), list3));
		assertTrue(check.checkEqual(left.getFromList(), tablemap));
		assertTrue(check.checkEqual(right.getFromList(), tablemap));
		assertTrue(check.checkEqual(left.getSelectAlias(), list4));
		assertTrue(check.checkEqual(right.getSelectAlias(), list4));
		Expression e9 = new ColumnNode("Boats.A");
		Expression e10 = new DoubleValue(10.0);
		Expression e11 = new GreaterThanOrEquals(e9,e10);
		Expression e12 = new ColumnNode("Boats.C");
		Expression e13 = new LongValue(1000);
		Expression e14 = new LessThan(e12,e13);
		assertTrue(check.checkEqual(left.getWhereExpression(), e11));
		assertTrue(check.checkEqual(right.getWhereExpression(), e14));
		assertTrue(check.checkEqual(left.getGroupByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(null, left.getHavingExpression()));
		assertTrue(check.isEqual(left.getOrderByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(left.isDescList(), null));
		assertTrue(check.checkEqual(right.getGroupByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(null, right.getHavingExpression()));
		assertTrue(check.isEqual(right.getOrderByElements(), new ArrayList<>()));
		assertTrue(check.checkEqual(right.isDescList(), null));
	}
	
}
