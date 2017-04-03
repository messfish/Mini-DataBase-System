package testcases;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import SQLExpression.AndOperator;
import SQLExpression.ColumnNode;
import SQLExpression.DateValue;
import SQLExpression.DoubleValue;
import SQLExpression.Equals;
import SQLExpression.Expression;
import SQLExpression.GreaterThan;
import SQLExpression.GreaterThanOrEquals;
import SQLExpression.InOperator;
import SQLExpression.LessThan;
import SQLExpression.LessThanOrEquals;
import SQLExpression.LikeOperator;
import SQLExpression.LongValue;
import SQLExpression.MultiAndOperator;
import SQLExpression.MultiOrOperator;
import SQLExpression.NotEquals;
import SQLExpression.NotOperator;
import SQLExpression.OrOperator;
import SQLExpression.Parenthesis;
import SQLExpression.StringValue;
import SQLExpression.TimeValue;
import SQLParser.CNFConverter;
import SQLParser.CheckEquals;

/**
 * this class is mainly used for testing whether we generate the 
 * correct CNF form of an expression tree. We use the name of 
 * variables that is reflected in the steps defined in the class.
 * @author messfish
 *
 */
public class CNFTest {

	private Expression original1, original2, original3, original4, original5;
	// these expressions stores the original expression tree.
	private Expression stepone1, stepone2, stepone3, stepone4, stepone5;
	// these expressions stores the step one of the expression tree.
	private Expression steptwo1, steptwo2, steptwo3, steptwo4, steptwo5;
	// these expression stores the step two of the expression tree.
	private Expression stepthree1, stepthree2, stepthree3, stepthree4, stepthree5;
	// these expression stores the step three of the expression tree.
	private Expression steplast1, steplast2, steplast3, steplast4, steplast5;
	// these expression stores the last step of the expression tree.
	private CNFConverter cnf1, cnf2, cnf3, cnf4, cnf5;
	// generate three CNF converter objects to handle the 
	// conversion of three expression trees.
	private CheckEquals check;
	
	/**
	 * Constructor: this constructor will be served to build several
	 * original construction tree and several expected trees that
	 * will be used in those test cases.
	 */
	public CNFTest() {
		/* Here is the original part of the expression tree. */
		buildOriginalExpression1();
		buildOriginalExpression2();
		buildOriginalExpression3();
		buildOriginalExpression4();
		buildOriginalExpression5();
		buildStepOneExpression1();
		buildStepOneExpression2();
		buildStepOneExpression3();
		buildStepOneExpression4();
		buildStepOneExpression5();
		buildStepTwoExpression1();
		buildStepTwoExpression2();
		buildStepTwoExpression3();
		buildStepTwoExpression4();
		buildStepTwoExpression5();
		buildStepThreeExpression1();
		buildStepThreeExpression2();
		buildStepThreeExpression3();
		buildStepThreeExpression4();
		buildStepThreeExpression5();
		buildStepLastExpression1();
		buildStepLastExpression2();
		buildStepLastExpression3();
		buildStepLastExpression4();
		buildStepLastExpression5();
		cnf1 = new CNFConverter();
		cnf2 = new CNFConverter();
		cnf3 = new CNFConverter();
		cnf4 = new CNFConverter();
		cnf5 = new CNFConverter();
		check = new CheckEquals();
	}
	
	@Test
	/**
	 * this method is used to check whether the checkEqual() method
	 * could return false if the two expression variables are pointing
	 * to the same expression tree.
	 */
	public void testStepZero() {
		assertFalse(check.checkEqual(original1, original1));
	}
	
	@Test
	/** 
	 * this method is used to check whether we clone the expression
	 * tree in the correct form that is specified in the clone class.
	 */
	public void testStepOne() {
		cnf1.reorder(original1);
		assertTrue(check.checkEqual(cnf1.getRoot(), stepone1));
		cnf2.reorder(original2);
		assertTrue(check.checkEqual(cnf2.getRoot(), stepone2));
		cnf3.reorder(original3);
		assertTrue(check.checkEqual(cnf3.getRoot(), stepone3));
		cnf4.reorder(original4);
		assertTrue(check.checkEqual(cnf4.getRoot(), stepone4));
		cnf5.reorder(original5);
		assertTrue(check.checkEqual(cnf5.getRoot(), stepone5));
	}
	
	@Test
	/**
	 * this method is used to check whether we push the not operators
	 * thoroughly. 
	 */
	public void testStepTwo() {
		cnf1.reorder(original1);
		cnf1.pushNotDown();
		assertTrue(check.checkEqual(cnf1.getRoot(), steptwo1));
		cnf2.reorder(original2);
		cnf2.pushNotDown();
		assertTrue(check.checkEqual(cnf2.getRoot(), steptwo2));
		cnf3.reorder(original3);
		cnf3.pushNotDown();
		assertTrue(check.checkEqual(cnf3.getRoot(), steptwo3));
		cnf4.reorder(original4);
		cnf4.pushNotDown();
		assertTrue(check.checkEqual(cnf4.getRoot(), steptwo4));
		cnf5.reorder(original5);
		cnf5.pushNotDown();
		assertTrue(check.checkEqual(cnf5.getRoot(), steptwo5));
	}
	
	@Test
	/**
	 * this method is used to check whether the expression tree is 
	 * reformed in the correct order.
	 */
	public void testStepThree() {
		cnf1.reorder(original1);
		cnf1.pushNotDown();
		cnf1.gather();
		assertTrue(check.checkEqual(cnf1.getRoot(), stepthree1));
		cnf2.reorder(original2);
		cnf2.pushNotDown();
		cnf2.gather();
		assertTrue(check.checkEqual(cnf2.getRoot(), stepthree2));
		cnf3.reorder(original3);
		cnf3.pushNotDown();
		cnf3.gather();
		assertTrue(check.checkEqual(cnf3.getRoot(), stepthree3));
		cnf4.reorder(original4);
		cnf4.pushNotDown();
		cnf4.gather();
		assertTrue(check.checkEqual(cnf4.getRoot(), stepthree4));
		cnf5.reorder(original5);
		cnf5.pushNotDown();
		cnf5.gather();
		assertTrue(check.checkEqual(cnf5.getRoot(), stepthree5));
	}

	@Test
	/**
	 * this method is used to check whether the expression tree is 
	 * converted into the CNF form.
	 */
	public void testLastStep() {
		cnf1.reorder(original1);
		cnf1.pushNotDown();
		cnf1.gather();
		cnf1.pushAndUp();
		assertTrue(check.checkEqual(cnf1.getRoot(), steplast1));
		cnf2.reorder(original2);
		cnf2.pushNotDown();
		cnf2.gather();
		cnf2.pushAndUp();
		assertTrue(check.checkEqual(cnf2.getRoot(), steplast2));
		cnf3.reorder(original3);
		cnf3.pushNotDown();
		cnf3.gather();
		cnf3.pushAndUp();
		assertTrue(check.checkEqual(cnf3.getRoot(), steplast3));
		cnf4.reorder(original4);
		cnf4.pushNotDown();
		cnf4.gather();
		cnf4.pushAndUp();
		assertTrue(check.checkEqual(cnf4.getRoot(), steplast4));
		cnf5.reorder(original5);
		cnf5.pushNotDown();
		cnf5.gather();
		cnf5.pushAndUp();
		assertTrue(check.checkEqual(cnf5.getRoot(), steplast5));
	}
	
	/**
	 * this method is used to test the full progress from the original
	 * expression tree to the expression tree in CNF form.
	 */
	@Test
	public void testAll() {
		cnf1.convert(original1);
		assertTrue(check.checkEqual(cnf1.getRoot(), steplast1));
		cnf2.convert(original2);
		assertTrue(check.checkEqual(cnf2.getRoot(), steplast2));
		cnf3.convert(original3);
		assertTrue(check.checkEqual(cnf3.getRoot(), steplast3));
		cnf4.convert(original4);
		assertTrue(check.checkEqual(cnf4.getRoot(), steplast4));
		cnf5.convert(original5);
		assertTrue(check.checkEqual(cnf5.getRoot(), steplast5));
	}
	
	/**
	 * this method is used to build the original expression tree 1.
	 * The purpose of this method is to check when there is a Not Operator
	 * at the root. Which means the root must be switched.
	 */
	private void buildOriginalExpression1() {
		Expression e1 = new DoubleValue(1.2);
		Expression e2 = new DoubleValue(2.3);
		Expression e3 = new DoubleValue(3.5);
		Expression e4 = new DoubleValue(4.6);
		Expression e5 = new DoubleValue(1.1);
		Expression e6 = new DoubleValue(2.5);
		Expression e7 = new DoubleValue(8.0);
		Expression e8 = new DoubleValue(7.2);
		Expression e9 = new LessThan(e1, e2);
		Expression e10 = new Equals(e3, e4);
		Expression e11 = new NotEquals(e5, e6);
		Expression e12 = new GreaterThan(e7, e8);
		Expression e13 = new OrOperator(e9, e10);
		Expression e14 = new OrOperator(e11, e12);
		Expression e15 = new AndOperator(e13, e14);
		Expression e16 = new Parenthesis(e15);
		original1 = new NotOperator(e16);
	}
	
	/**
	 * this method is used to build the original expression tree 2.
	 * The purpose is to test the double negation law. As you can 
	 * see when you build the tree, there will be two Not Operators 
	 * together on the line. It is there when we use the double negation law.
	 */
	private void buildOriginalExpression2() {
		Expression e1 = new DoubleValue(1.1);
		Expression e2 = new DoubleValue(2.3);
		Expression e3 = new DoubleValue(3.3);
		Expression e4 = new DoubleValue(4.5);
		Expression e5 = new ColumnNode("S.A");
		Expression e6 = new StringValue("\"%%%\"");
		Expression e7 = new ColumnNode("S.B");
		Expression e8 = new StringValue("orz");
		Expression e9 = new GreaterThanOrEquals(e1,e2);
		Expression e10 = new LessThanOrEquals(e3,e4);
		Expression e11 = new LikeOperator(e5,e6);
		Expression e12 = new InOperator(e7,e8);
		Expression e13 = new NotOperator(e9);
		Expression e14 = new OrOperator(e13,e10);
		Expression e15 = new NotOperator(e14);
		Expression e16 = new AndOperator(e11,e12);
		Expression e17 = new Parenthesis(e16);
		Expression e18 = new OrOperator(e15,e17);
		original2 = new Parenthesis(e18);
	}
	
	/**
	 * this method is used to build the original expression tree 3.
	 * This is the case when we test a more complex tree structure,
	 * Notice you could see the amount of line to build up the CNF tree.
	 * You could tell how complicated the CNF could be.
	 */
	private void buildOriginalExpression3() {
		Expression e1 = new DoubleValue(3.0);
		Expression e2 = new DoubleValue(4.0);
		Expression e3 = new DoubleValue(5.0);
		Expression e4 = new DoubleValue(6.0);
		Expression e5 = new DoubleValue(7.0);
		Expression e6 = new DoubleValue(8.0);
		Expression e7 = new DoubleValue(9.0);
		Expression e8 = new DoubleValue(10.0);
		Expression e9 = new DoubleValue(11.0);
		Expression e10 = new DoubleValue(12.0);
		Expression e11 = new DoubleValue(13.0);
		Expression e12 = new DoubleValue(14.0);
		Expression e13 = new DoubleValue(15.0);
		Expression e14 = new DoubleValue(16.0);
		Expression e15 = new DoubleValue(17.0);
		Expression e16 = new DoubleValue(18.0);
		Expression e17 = new DoubleValue(19.0);
		Expression e18 = new DoubleValue(20.0);
		Expression e19 = new GreaterThanOrEquals(e1, e2);
		Expression e20 = new LessThanOrEquals(e3, e4);
		Expression e21 = new LessThan(e5, e6);
		Expression e22 = new GreaterThan(e7, e8);
		Expression e23 = new Equals(e9, e10);
		Expression e24 = new NotEquals(e11, e12);
		Expression e25 = new LikeOperator(e13, e14);
		Expression e26 = new InOperator(e15, e16);
		Expression e27 = new GreaterThan(e17, e18);
		Expression e28 = new AndOperator(e19, e20);
		Expression e29 = new AndOperator(e21, e22);
		Expression e30 = new OrOperator(e26, e27);
		Expression e31 = new AndOperator(e29, e23);
		Expression e32 = new AndOperator(e25, e30);
		Expression e33 = new OrOperator(e24, e32);
		Expression e34 = new NotOperator(e33);
		Expression e35 = new OrOperator(e31, e34);
		Expression e36 = new Parenthesis(e28);
		Expression e37 = new Parenthesis(e35);
		original3 = new OrOperator(e36, e37);
	}
	
	/**
	 * This method is used for building the original expression tree 4.
	 * This is the case when we test a very simple tree structure that 
	 * has neither AND operator or OR operator.
	 */
	private void buildOriginalExpression4() {
		Expression e1 = new ColumnNode("S.D");
		Expression e2 = new DateValue("2017/03/25");
		Expression e3 = new GreaterThan(e1,e2);
		original4 = new NotOperator(e3);
	}
	
	/**
	 * This method is used for building the original expression tree 5.
	 * This is the case when we test the tree that only contains AND
	 * operator without having an OR operator.
	 */
	private void buildOriginalExpression5() {
		Expression e1 = new ColumnNode("S.A");
		Expression e2 = new DoubleValue(3.5);
		Expression e3 = new ColumnNode("S.B");
		Expression e4 = new LongValue(4);
		Expression e5 = new ColumnNode("S.C");
		Expression e6 = new TimeValue("12:04:34");
		Expression e7 = new ColumnNode("S.D");
		Expression e8 = new StringValue("sddsds");
		Expression e9 = new GreaterThan(e1,e2);
		Expression e10 = new LessThan(e3,e4);
		Expression e11 = new LikeOperator(e5,e6);
		Expression e12 = new Equals(e7,e8);
		Expression e13 = new AndOperator(e9,e10);
		Expression e14 = new OrOperator(e11,e12);
		Expression e15 = new NotOperator(e13);
		Expression e16 = new OrOperator(e15,e14);
		original5 = new NotOperator(e16);
	}
	
	/**
	 * this method is used to build the step one expression tree 1.
	 */
	private void buildStepOneExpression1() {
		Expression e1 = new DoubleValue(1.2);
		Expression e2 = new DoubleValue(2.3);
		Expression e3 = new DoubleValue(3.5);
		Expression e4 = new DoubleValue(4.6);
		Expression e5 = new DoubleValue(1.1);
		Expression e6 = new DoubleValue(2.5);
		Expression e7 = new DoubleValue(8.0);
		Expression e8 = new DoubleValue(7.2);
		Expression e9 = new LessThan(e1, e2);
		Expression e10 = new Equals(e3, e4);
		Expression e11 = new NotEquals(e5, e6);
		Expression e12 = new GreaterThan(e7, e8);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e9);
		list1.add(e10);
		Expression e13 = new MultiOrOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e11);
		list2.add(e12);
		Expression e14 = new MultiOrOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e13);
		list3.add(e14);
		Expression e15 = new MultiAndOperator(list3);
		stepone1 = new NotOperator(e15);
	}
	
	/**
	 * this method is used to build the step one expression tree 2.
	 */
	private void buildStepOneExpression2() {
		Expression e1 = new DoubleValue(1.1);
		Expression e2 = new DoubleValue(2.3);
		Expression e3 = new DoubleValue(3.3);
		Expression e4 = new DoubleValue(4.5);
		Expression e5 = new ColumnNode("S.A");
		Expression e6 = new StringValue("\"%%%\"");
		Expression e7 = new ColumnNode("S.B");
		Expression e8 = new StringValue("orz");
		Expression e9 = new GreaterThanOrEquals(e1,e2);
		Expression e10 = new LessThanOrEquals(e3,e4);
		Expression e11 = new LikeOperator(e5,e6);
		Expression e12 = new InOperator(e7,e8);
		Expression e13 = new NotOperator(e9);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e13);
		list1.add(e10);
		Expression e14 = new MultiOrOperator(list1);
		Expression e15 = new NotOperator(e14);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e11);
		list2.add(e12);
		Expression e16 = new MultiAndOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e15);
		list3.add(e16);
		stepone2 = new MultiOrOperator(list3);
	}
	
	/**
	 * this method is used to build the step one expression tree 3.
	 */
	private void buildStepOneExpression3() {
		Expression e1 = new DoubleValue(3.0);
		Expression e2 = new DoubleValue(4.0);
		Expression e3 = new DoubleValue(5.0);
		Expression e4 = new DoubleValue(6.0);
		Expression e5 = new DoubleValue(7.0);
		Expression e6 = new DoubleValue(8.0);
		Expression e7 = new DoubleValue(9.0);
		Expression e8 = new DoubleValue(10.0);
		Expression e9 = new DoubleValue(11.0);
		Expression e10 = new DoubleValue(12.0);
		Expression e11 = new DoubleValue(13.0);
		Expression e12 = new DoubleValue(14.0);
		Expression e13 = new DoubleValue(15.0);
		Expression e14 = new DoubleValue(16.0);
		Expression e15 = new DoubleValue(17.0);
		Expression e16 = new DoubleValue(18.0);
		Expression e17 = new DoubleValue(19.0);
		Expression e18 = new DoubleValue(20.0);
		Expression e19 = new GreaterThanOrEquals(e1, e2);
		Expression e20 = new LessThanOrEquals(e3, e4);
		Expression e21 = new LessThan(e5, e6);
		Expression e22 = new GreaterThan(e7, e8);
		Expression e23 = new Equals(e9, e10);
		Expression e24 = new NotEquals(e11, e12);
		Expression e25 = new LikeOperator(e13, e14);
		Expression e26 = new InOperator(e15, e16);
		Expression e27 = new GreaterThan(e17, e18);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e19);
		list1.add(e20);
		Expression e28 = new MultiAndOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e21);
		list2.add(e22);
		Expression e29 = new MultiAndOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e26);
		list3.add(e27);
		Expression e30 = new MultiOrOperator(list3);
		List<Expression> list4 = new ArrayList<>();
		list4.add(e29);
		list4.add(e23);
		Expression e31 = new MultiAndOperator(list4);
		List<Expression> list5 = new ArrayList<>();
		list5.add(e25);
		list5.add(e30);
		Expression e32 = new MultiAndOperator(list5);
		List<Expression> list6 = new ArrayList<>();
		list6.add(e24);
		list6.add(e32);
		Expression e33 = new MultiOrOperator(list6);
		Expression e34 = new NotOperator(e33);
		List<Expression> list7 = new ArrayList<>();
		list7.add(e31);
		list7.add(e34);
		Expression e35 = new MultiOrOperator(list7);
		List<Expression> list8 = new ArrayList<>();
		list8.add(e28);
		list8.add(e35);
		stepone3 = new MultiOrOperator(list8);
	}
	
	/**
	 * this method is used to build the step one expression tree 4.
	 */
	private void buildStepOneExpression4() {
		Expression e1 = new ColumnNode("S.D");
		Expression e2 = new DateValue("2017/03/25");
		Expression e3 = new GreaterThan(e1,e2);
		stepone4 = new NotOperator(e3);
	}
	
	/**
	 * this method is used to build the step one expression tree 1.
	 */
	private void buildStepOneExpression5() {
		Expression e1 = new ColumnNode("S.A");
		Expression e2 = new DoubleValue(3.5);
		Expression e3 = new ColumnNode("S.B");
		Expression e4 = new LongValue(4);
		Expression e5 = new ColumnNode("S.C");
		Expression e6 = new TimeValue("12:04:34");
		Expression e7 = new ColumnNode("S.D");
		Expression e8 = new StringValue("sddsds");
		Expression e9 = new GreaterThan(e1,e2);
		Expression e10 = new LessThan(e3,e4);
		Expression e11 = new LikeOperator(e5,e6);
		Expression e12 = new Equals(e7,e8);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e9);
		list1.add(e10);
		Expression e13 = new MultiAndOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e11);
		list2.add(e12);
		Expression e14 = new MultiOrOperator(list2);
		Expression e15 = new NotOperator(e13);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e15);
		list3.add(e14);
		Expression e16 = new MultiOrOperator(list3);
		stepone5 = new NotOperator(e16);
	}
	
	/**
	 * this method is used to build the step two expression tree 1.
	 */
	private void buildStepTwoExpression1() {
		Expression e1 = new DoubleValue(1.2);
		Expression e2 = new DoubleValue(2.3);
		Expression e3 = new DoubleValue(3.5);
		Expression e4 = new DoubleValue(4.6);
		Expression e5 = new DoubleValue(1.1);
		Expression e6 = new DoubleValue(2.5);
		Expression e7 = new DoubleValue(8.0);
		Expression e8 = new DoubleValue(7.2);
		Expression e9 = new LessThan(e1, e2);
		Expression e10 = new Equals(e3, e4);
		Expression e11 = new NotEquals(e5, e6);
		Expression e12 = new GreaterThan(e7, e8);
		Expression e13 = new NotOperator(e9);
		Expression e14 = new NotOperator(e10);
		Expression e15 = new NotOperator(e11);
		Expression e16 = new NotOperator(e12);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e13);
		list1.add(e14);
		Expression e17 = new MultiAndOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e15);
		list2.add(e16);
		Expression e18 = new MultiAndOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e17);
		list3.add(e18);
		steptwo1 = new MultiOrOperator(list3);
	}
	
	/**
	 * this method is used to build the step two expression tree 2.
	 */
	private void buildStepTwoExpression2() {
		Expression e1 = new DoubleValue(1.1);
		Expression e2 = new DoubleValue(2.3);
		Expression e3 = new DoubleValue(3.3);
		Expression e4 = new DoubleValue(4.5);
		Expression e5 = new ColumnNode("S.A");
		Expression e6 = new StringValue("\"%%%\"");
		Expression e7 = new ColumnNode("S.B");
		Expression e8 = new StringValue("orz");
		Expression e9 = new GreaterThanOrEquals(e1,e2);
		Expression e10 = new LessThanOrEquals(e3,e4);
		Expression e11 = new LikeOperator(e5,e6);
		Expression e12 = new InOperator(e7,e8);
		Expression e13 = new NotOperator(e10);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e9);
		list1.add(e13);
		Expression e14 = new MultiAndOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e11);
		list2.add(e12);
		Expression e15 = new MultiAndOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e14);
		list3.add(e15);
		steptwo2 = new MultiOrOperator(list3);
	}
	
	/**
	 * this method is used to build the step two expression tree 3.
	 */
	private void buildStepTwoExpression3 () {
		Expression e1 = new DoubleValue(3.0);
		Expression e2 = new DoubleValue(4.0);
		Expression e3 = new DoubleValue(5.0);
		Expression e4 = new DoubleValue(6.0);
		Expression e5 = new DoubleValue(7.0);
		Expression e6 = new DoubleValue(8.0);
		Expression e7 = new DoubleValue(9.0);
		Expression e8 = new DoubleValue(10.0);
		Expression e9 = new DoubleValue(11.0);
		Expression e10 = new DoubleValue(12.0);
		Expression e11 = new DoubleValue(13.0);
		Expression e12 = new DoubleValue(14.0);
		Expression e13 = new DoubleValue(15.0);
		Expression e14 = new DoubleValue(16.0);
		Expression e15 = new DoubleValue(17.0);
		Expression e16 = new DoubleValue(18.0);
		Expression e17 = new DoubleValue(19.0);
		Expression e18 = new DoubleValue(20.0);
		Expression e19 = new GreaterThanOrEquals(e1, e2);
		Expression e20 = new LessThanOrEquals(e3, e4);
		Expression e21 = new LessThan(e5, e6);
		Expression e22 = new GreaterThan(e7, e8);
		Expression e23 = new Equals(e9, e10);
		Expression e24 = new NotEquals(e11, e12);
		Expression e25 = new LikeOperator(e13, e14);
		Expression e26 = new InOperator(e15, e16);
		Expression e27 = new GreaterThan(e17, e18);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e19);
		list1.add(e20);
		Expression e28 = new MultiAndOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e21);
		list2.add(e22);
		Expression e29 = new MultiAndOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e29);
		list3.add(e23);
		Expression e30 = new MultiAndOperator(list3);
		Expression e31 = new NotOperator(e24);
		Expression e32 = new NotOperator(e25);
		Expression e37 = new NotOperator(e26);
		Expression e38 = new NotOperator(e27);
		List<Expression> list4 = new ArrayList<>();
		list4.add(e37);
		list4.add(e38);
		Expression e33 = new MultiAndOperator(list4);
		List<Expression> list5 = new ArrayList<>();
		list5.add(e32);
		list5.add(e33);
		Expression e34 = new MultiOrOperator(list5);
		List<Expression> list6 = new ArrayList<>();
		list6.add(e31);
		list6.add(e34);
		Expression e35 = new MultiAndOperator(list6);
		List<Expression> list7 = new ArrayList<>();
		list7.add(e30);
		list7.add(e35);
		Expression e36 = new MultiOrOperator(list7);
		List<Expression> list8 = new ArrayList<>();
		list8.add(e28);
		list8.add(e36);
		steptwo3 = new MultiOrOperator(list8);
	}
	
	/**
	 * this method is used to build the step two expression tree 4.
	 */
	private void buildStepTwoExpression4() {
		Expression e1 = new ColumnNode("S.D");
		Expression e2 = new DateValue("2017/03/25");
		Expression e3 = new GreaterThan(e1,e2);
		steptwo4 = new NotOperator(e3);
	}
	
	/**
	 * this method is used to build the step two expression tree 4.
	 */
	private void buildStepTwoExpression5() {
		Expression e1 = new ColumnNode("S.A");
		Expression e2 = new DoubleValue(3.5);
		Expression e3 = new ColumnNode("S.B");
		Expression e4 = new LongValue(4);
		Expression e5 = new ColumnNode("S.C");
		Expression e6 = new TimeValue("12:04:34");
		Expression e7 = new ColumnNode("S.D");
		Expression e8 = new StringValue("sddsds");
		Expression e9 = new GreaterThan(e1,e2);
		Expression e10 = new LessThan(e3,e4);
		Expression e11 = new LikeOperator(e5,e6);
		Expression e12 = new Equals(e7,e8);
		Expression e13 = new NotOperator(e11);
		Expression e14 = new NotOperator(e12);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e9);
		list1.add(e10);
		Expression e15 = new MultiAndOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e13);
		list2.add(e14);
		Expression e16 = new MultiAndOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e15);
		list3.add(e16);
		steptwo5 = new MultiAndOperator(list3);
	}
	
	/**
	 * this method is used to build the step three expression tree 1.
	 */
	private void buildStepThreeExpression1() {
		Expression e1 = new DoubleValue(1.2);
		Expression e2 = new DoubleValue(2.3);
		Expression e3 = new DoubleValue(3.5);
		Expression e4 = new DoubleValue(4.6);
		Expression e5 = new DoubleValue(1.1);
		Expression e6 = new DoubleValue(2.5);
		Expression e7 = new DoubleValue(8.0);
		Expression e8 = new DoubleValue(7.2);
		Expression e9 = new LessThan(e1, e2);
		Expression e10 = new Equals(e3, e4);
		Expression e11 = new NotEquals(e5, e6);
		Expression e12 = new GreaterThan(e7, e8);
		Expression e13 = new NotOperator(e9);
		Expression e14 = new NotOperator(e10);
		Expression e15 = new NotOperator(e11);
		Expression e16 = new NotOperator(e12);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e13);
		list1.add(e14);
		Expression e17 = new MultiAndOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e15);
		list2.add(e16);
		Expression e18 = new MultiAndOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e17);
		list3.add(e18);
		stepthree1 = new MultiOrOperator(list3);
	}
	
	/**
	 * this method is used to build the step three expression tree 2.
	 */
	private void buildStepThreeExpression2() {
		Expression e1 = new DoubleValue(1.1);
		Expression e2 = new DoubleValue(2.3);
		Expression e3 = new DoubleValue(3.3);
		Expression e4 = new DoubleValue(4.5);
		Expression e5 = new ColumnNode("S.A");
		Expression e6 = new StringValue("\"%%%\"");
		Expression e7 = new ColumnNode("S.B");
		Expression e8 = new StringValue("orz");
		Expression e9 = new GreaterThanOrEquals(e1,e2);
		Expression e10 = new LessThanOrEquals(e3,e4);
		Expression e11 = new LikeOperator(e5,e6);
		Expression e12 = new InOperator(e7,e8);
		Expression e13 = new NotOperator(e10);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e9);
		list1.add(e13);
		Expression e14 = new MultiAndOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e11);
		list2.add(e12);
		Expression e15 = new MultiAndOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e14);
		list3.add(e15);
		stepthree2 = new MultiOrOperator(list3);
	}
	
	/**
	 * this method is used to build the step three expression tree 3.
	 */
	private void buildStepThreeExpression3() {
		Expression e1 = new DoubleValue(3.0);
		Expression e2 = new DoubleValue(4.0);
		Expression e3 = new DoubleValue(5.0);
		Expression e4 = new DoubleValue(6.0);
		Expression e5 = new DoubleValue(7.0);
		Expression e6 = new DoubleValue(8.0);
		Expression e7 = new DoubleValue(9.0);
		Expression e8 = new DoubleValue(10.0);
		Expression e9 = new DoubleValue(11.0);
		Expression e10 = new DoubleValue(12.0);
		Expression e11 = new DoubleValue(13.0);
		Expression e12 = new DoubleValue(14.0);
		Expression e13 = new DoubleValue(15.0);
		Expression e14 = new DoubleValue(16.0);
		Expression e15 = new DoubleValue(17.0);
		Expression e16 = new DoubleValue(18.0);
		Expression e17 = new DoubleValue(19.0);
		Expression e18 = new DoubleValue(20.0);
		Expression e19 = new GreaterThanOrEquals(e1, e2);
		Expression e20 = new LessThanOrEquals(e3, e4);
		Expression e21 = new LessThan(e5, e6);
		Expression e22 = new GreaterThan(e7, e8);
		Expression e23 = new Equals(e9, e10);
		Expression e24 = new NotEquals(e11, e12);
		Expression e25 = new LikeOperator(e13, e14);
		Expression e26 = new InOperator(e15, e16);
		Expression e27 = new GreaterThan(e17, e18);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e19);
		list1.add(e20);
		Expression e28 = new MultiAndOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e21);
		list2.add(e22);
		list2.add(e23);
		Expression e29 = new MultiAndOperator(list2);
		Expression e30 = new NotOperator(e26);
		Expression e31 = new NotOperator(e27);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e30);
		list3.add(e31);
		Expression e32 = new MultiAndOperator(list3);
		Expression e33 = new NotOperator(e25);
		List<Expression> list4 = new ArrayList<>();
		list4.add(e33);
		list4.add(e32);
		Expression e34 = new MultiOrOperator(list4);
		Expression e35 = new NotOperator(e24);
		List<Expression> list5 = new ArrayList<>();
		list5.add(e35);
		list5.add(e34);
		Expression e36 = new MultiAndOperator(list5);
		List<Expression> list6 = new ArrayList<>();
		list6.add(e28);
		list6.add(e29);
		list6.add(e36);
		stepthree3 = new MultiOrOperator(list6);
	}
	
	/**
	 * this method is used to build the step three expression tree 4.
	 */
	private void buildStepThreeExpression4() {
		Expression e1 = new ColumnNode("S.D");
		Expression e2 = new DateValue("2017/03/25");
		Expression e3 = new GreaterThan(e1,e2);
		stepthree4 = new NotOperator(e3);
	}
	
	/**
	 * this method is used to build the step three expression tree 5.
	 */
	private void buildStepThreeExpression5() {
		Expression e1 = new ColumnNode("S.A");
		Expression e2 = new DoubleValue(3.5);
		Expression e3 = new ColumnNode("S.B");
		Expression e4 = new LongValue(4);
		Expression e5 = new ColumnNode("S.C");
		Expression e6 = new TimeValue("12:04:34");
		Expression e7 = new ColumnNode("S.D");
		Expression e8 = new StringValue("sddsds");
		Expression e9 = new GreaterThan(e1,e2);
		Expression e10 = new LessThan(e3,e4);
		Expression e11 = new LikeOperator(e5,e6);
		Expression e12 = new Equals(e7,e8);
		Expression e13 = new NotOperator(e11);
		Expression e14 = new NotOperator(e12);
		List<Expression> list = new ArrayList<>();
		list.add(e9);
		list.add(e10);
		list.add(e13);
		list.add(e14);
		stepthree5 = new MultiAndOperator(list);
	}
	
	/**
	 * this method is used to build the last step expression tree.
	 */
	private void buildStepLastExpression1() {
		Expression e1 = new DoubleValue(1.2);
		Expression e2 = new DoubleValue(2.3);
		Expression e3 = new DoubleValue(1.1);
		Expression e4 = new DoubleValue(2.5);
		Expression e5 = new DoubleValue(1.2);
		Expression e6 = new DoubleValue(2.3);
		Expression e7 = new DoubleValue(8.0);
		Expression e8 = new DoubleValue(7.2);
		Expression e9 = new DoubleValue(3.5);
		Expression e10 = new DoubleValue(4.6);
		Expression e11 = new DoubleValue(1.1);
		Expression e12 = new DoubleValue(2.5);
		Expression e13 = new DoubleValue(3.5);
		Expression e14 = new DoubleValue(4.6);
		Expression e15 = new DoubleValue(8.0);
		Expression e16 = new DoubleValue(7.2);
		Expression e17 = new LessThan(e1, e2);
		Expression e18 = new NotEquals(e3, e4);
		Expression e19 = new LessThan(e5, e6);
		Expression e20 = new GreaterThan(e7, e8);
		Expression e21 = new Equals(e9, e10);
		Expression e22 = new NotEquals(e11, e12);
		Expression e23 = new Equals(e13, e14);
		Expression e24 = new GreaterThan(e15, e16);
		Expression e25 = new NotOperator(e17);
		Expression e26 = new NotOperator(e18);
		Expression e27 = new NotOperator(e19);
		Expression e28 = new NotOperator(e20);
		Expression e29 = new NotOperator(e21);
		Expression e30 = new NotOperator(e22);
		Expression e31 = new NotOperator(e23);
		Expression e32 = new NotOperator(e24);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e25);
		list1.add(e26);
		Expression e33 = new MultiOrOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e27);
		list2.add(e28);
		Expression e34 = new MultiOrOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e29);
		list3.add(e30);
		Expression e35 = new MultiOrOperator(list3);
		List<Expression> list4 = new ArrayList<>();
		list4.add(e31);
		list4.add(e32);
		Expression e36 = new MultiOrOperator(list4);
		List<Expression> list5 = new ArrayList<>();
		list5.add(e33);
		list5.add(e34);
		list5.add(e35);
		list5.add(e36);
		steplast1 = new MultiAndOperator(list5);
	}
	
	/**
	 * this method is used to build the last step expression tree 2.
	 */
	private void buildStepLastExpression2() {
		Expression e1 = new DoubleValue(1.1);
		Expression e2 = new DoubleValue(2.3);
		Expression e3 = new ColumnNode("S.A");
		Expression e4 = new StringValue("\"%%%\"");
		Expression e5 = new DoubleValue(1.1);
		Expression e6 = new DoubleValue(2.3);
		Expression e7 = new ColumnNode("S.B");
		Expression e8 = new StringValue("orz");
		Expression e9 = new DoubleValue(3.3);
		Expression e10 = new DoubleValue(4.5);
		Expression e11 = new ColumnNode("S.A");
		Expression e12 = new StringValue("\"%%%\"");
		Expression e13 = new DoubleValue(3.3);
		Expression e14 = new DoubleValue(4.5);
		Expression e15 = new ColumnNode("S.B");
		Expression e16 = new StringValue("orz");
		Expression e17 = new GreaterThanOrEquals(e1, e2);
		Expression e18 = new LikeOperator(e3, e4);
		Expression e19 = new GreaterThanOrEquals(e5, e6);
		Expression e20 = new InOperator(e7, e8);
		Expression e21 = new LessThanOrEquals(e9, e10);
		Expression e22 = new LikeOperator(e11, e12);
		Expression e23 = new LessThanOrEquals(e13, e14);
		Expression e24 = new InOperator(e15, e16);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e17);
		list1.add(e18);
		Expression e25 = new MultiOrOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e19);
		list2.add(e20);
		Expression e26 = new MultiOrOperator(list2);
		Expression e27 = new NotOperator(e21);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e27);
		list3.add(e22);
		Expression e28 = new MultiOrOperator(list3);
		Expression e29 = new NotOperator(e23);
		List<Expression> list4 = new ArrayList<>();
		list4.add(e29);
		list4.add(e24);
		Expression e30 = new MultiOrOperator(list4);
		List<Expression> list5 = new ArrayList<>();
		list5.add(e25);
		list5.add(e26);
		list5.add(e28);
		list5.add(e30);
		steplast2 = new MultiAndOperator(list5);
	}
	
	/**
	 * this method is used to build the last step expression tree 3.
	 */
	private void buildStepLastExpression3() {
		Expression e1 = new DoubleValue(3.0);
		Expression e2 = new DoubleValue(4.0);
		Expression e3 = new DoubleValue(7.0);
		Expression e4 = new DoubleValue(8.0);
		Expression e5 = new DoubleValue(13.0);
		Expression e6 = new DoubleValue(14.0);
		Expression e7 = new DoubleValue(3.0);
		Expression e8 = new DoubleValue(4.0);
		Expression e9 = new DoubleValue(7.0);
		Expression e10 = new DoubleValue(8.0);
		Expression e11 = new DoubleValue(15.0);
		Expression e12 = new DoubleValue(16.0);
		Expression e13 = new DoubleValue(17.0);
		Expression e14 = new DoubleValue(18.0);
		Expression e15 = new DoubleValue(3.0);
		Expression e16 = new DoubleValue(4.0);
		Expression e17 = new DoubleValue(7.0);
		Expression e18 = new DoubleValue(8.0);
		Expression e19 = new DoubleValue(15.0);
		Expression e20 = new DoubleValue(16.0);
		Expression e21 = new DoubleValue(19.0);
		Expression e22 = new DoubleValue(20.0);
		Expression e23 = new DoubleValue(3.0);
		Expression e24 = new DoubleValue(4.0);
		Expression e25 = new DoubleValue(9.0);
		Expression e26 = new DoubleValue(10.0);
		Expression e27 = new DoubleValue(13.0);
		Expression e28 = new DoubleValue(14.0);
		Expression e29 = new DoubleValue(3.0);
		Expression e30 = new DoubleValue(4.0);
		Expression e31 = new DoubleValue(9.0);
		Expression e32 = new DoubleValue(10.0);
		Expression e33 = new DoubleValue(15.0);
		Expression e34 = new DoubleValue(16.0);
		Expression e35 = new DoubleValue(17.0);
		Expression e36 = new DoubleValue(18.0);
		Expression e37 = new DoubleValue(3.0);
		Expression e38 = new DoubleValue(4.0);
		Expression e39 = new DoubleValue(9.0);
		Expression e40 = new DoubleValue(10.0);
		Expression e41 = new DoubleValue(15.0);
		Expression e42 = new DoubleValue(16.0);
		Expression e43 = new DoubleValue(19.0);
		Expression e44 = new DoubleValue(20.0);
		Expression e45 = new DoubleValue(3.0);
		Expression e46 = new DoubleValue(4.0);
		Expression e47 = new DoubleValue(11.0);
		Expression e48 = new DoubleValue(12.0);
		Expression e49 = new DoubleValue(13.0);
		Expression e50 = new DoubleValue(14.0);
		Expression e51 = new DoubleValue(3.0);
		Expression e52 = new DoubleValue(4.0);
		Expression e53 = new DoubleValue(11.0);
		Expression e54 = new DoubleValue(12.0);
		Expression e55 = new DoubleValue(15.0);
		Expression e56 = new DoubleValue(16.0);
		Expression e57 = new DoubleValue(17.0);
		Expression e58 = new DoubleValue(18.0);
		Expression e59 = new DoubleValue(3.0);
		Expression e60 = new DoubleValue(4.0);
		Expression e61 = new DoubleValue(11.0);
		Expression e62 = new DoubleValue(12.0);
		Expression e63 = new DoubleValue(15.0);
		Expression e64 = new DoubleValue(16.0);
		Expression e65 = new DoubleValue(19.0);
		Expression e66 = new DoubleValue(20.0);
		Expression e67 = new DoubleValue(5.0);
		Expression e68 = new DoubleValue(6.0);
		Expression e69 = new DoubleValue(7.0);
		Expression e70 = new DoubleValue(8.0);
		Expression e71 = new DoubleValue(13.0);
		Expression e72 = new DoubleValue(14.0);
		Expression e73 = new DoubleValue(5.0);
		Expression e74 = new DoubleValue(6.0);
		Expression e75 = new DoubleValue(7.0);
		Expression e76 = new DoubleValue(8.0);
		Expression e77 = new DoubleValue(15.0);
		Expression e78 = new DoubleValue(16.0);
		Expression e79 = new DoubleValue(17.0);
		Expression e80 = new DoubleValue(18.0);
		Expression e81 = new DoubleValue(5.0);
		Expression e82 = new DoubleValue(6.0);
		Expression e83 = new DoubleValue(7.0);
		Expression e84 = new DoubleValue(8.0);
		Expression e85 = new DoubleValue(15.0);
		Expression e86 = new DoubleValue(16.0);
		Expression e87 = new DoubleValue(19.0);
		Expression e88 = new DoubleValue(20.0);
		Expression e89 = new DoubleValue(5.0);
		Expression e90 = new DoubleValue(6.0);
		Expression e91 = new DoubleValue(9.0);
		Expression e92 = new DoubleValue(10.0);
		Expression e93 = new DoubleValue(13.0);
		Expression e94 = new DoubleValue(14.0);
		Expression e95 = new DoubleValue(5.0);
		Expression e96 = new DoubleValue(6.0);
		Expression e97 = new DoubleValue(9.0);
		Expression e98 = new DoubleValue(10.0);
		Expression e99 = new DoubleValue(15.0);
		Expression e100 = new DoubleValue(16.0);
		Expression e101 = new DoubleValue(17.0);
		Expression e102 = new DoubleValue(18.0);
		Expression e103 = new DoubleValue(5.0);
		Expression e104 = new DoubleValue(6.0);
		Expression e105 = new DoubleValue(9.0);
		Expression e106 = new DoubleValue(10.0);
		Expression e107 = new DoubleValue(15.0);
		Expression e108 = new DoubleValue(16.0);
		Expression e109 = new DoubleValue(19.0);
		Expression e110 = new DoubleValue(20.0);
		Expression e111 = new DoubleValue(5.0);
		Expression e112 = new DoubleValue(6.0);
		Expression e113 = new DoubleValue(11.0);
		Expression e114 = new DoubleValue(12.0);
		Expression e115 = new DoubleValue(13.0);
		Expression e116 = new DoubleValue(14.0);
		Expression e117 = new DoubleValue(5.0);
		Expression e118 = new DoubleValue(6.0);
		Expression e119 = new DoubleValue(11.0);
		Expression e120 = new DoubleValue(12.0);
		Expression e121 = new DoubleValue(15.0);
		Expression e122 = new DoubleValue(16.0);
		Expression e123 = new DoubleValue(17.0);
		Expression e124 = new DoubleValue(18.0);
		Expression e125 = new DoubleValue(5.0);
		Expression e126 = new DoubleValue(6.0);
		Expression e127 = new DoubleValue(11.0);
		Expression e128 = new DoubleValue(12.0);
		Expression e129 = new DoubleValue(15.0);
		Expression e130 = new DoubleValue(16.0);
		Expression e131 = new DoubleValue(19.0);
		Expression e132 = new DoubleValue(20.0);
		Expression e133 = new GreaterThanOrEquals(e1, e2);
		Expression e134 = new LessThan(e3, e4);
		Expression e135 = new NotEquals(e5, e6);
		Expression e136 = new NotOperator(e135);
		Expression e137 = new GreaterThanOrEquals(e7, e8);
		Expression e138 = new LessThan(e9, e10);
		Expression e139 = new LikeOperator(e11, e12);
		Expression e140 = new NotOperator(e139);
		Expression e141 = new InOperator(e13, e14);
		Expression e142 = new NotOperator(e141);
		Expression e143 = new GreaterThanOrEquals(e15, e16);
		Expression e144 = new LessThan(e17, e18);
		Expression e145 = new LikeOperator(e19, e20);
		Expression e146 = new NotOperator(e145);
		Expression e147 = new GreaterThan(e21, e22);
		Expression e148 = new NotOperator(e147);
		Expression e149 = new GreaterThanOrEquals(e23, e24);
		Expression e150 = new GreaterThan(e25, e26);
		Expression e151 = new NotEquals(e27, e28);
		Expression e152 = new NotOperator(e151);
		Expression e153 = new GreaterThanOrEquals(e29, e30);
		Expression e154 = new GreaterThan(e31, e32);
		Expression e155 = new LikeOperator(e33, e34);
		Expression e156 = new NotOperator(e155);
		Expression e157 = new InOperator(e35, e36);
		Expression e158 = new NotOperator(e157);
		Expression e159 = new GreaterThanOrEquals(e37, e38);
		Expression e160 = new GreaterThan(e39, e40);
		Expression e161 = new LikeOperator(e41, e42);
		Expression e162 = new NotOperator(e161);
		Expression e163 = new GreaterThan(e43, e44);
		Expression e164 = new NotOperator(e163);
		Expression e165 = new GreaterThanOrEquals(e45, e46);
		Expression e166 = new Equals(e47, e48);
		Expression e167 = new NotEquals(e49, e50);
		Expression e168 = new NotOperator(e167);
		Expression e169 = new GreaterThanOrEquals(e51, e52);
		Expression e170 = new Equals(e53, e54);
		Expression e171 = new LikeOperator(e55, e56);
		Expression e172 = new NotOperator(e171);
		Expression e173 = new InOperator(e57, e58);
		Expression e174 = new NotOperator(e173);
		Expression e175 = new GreaterThanOrEquals(e59, e60);
		Expression e176 = new Equals(e61, e62);
		Expression e177 = new LikeOperator(e63, e64);
		Expression e178 = new NotOperator(e177);
		Expression e179 = new GreaterThan(e65, e66);
		Expression e180 = new NotOperator(e179);
		Expression e181 = new LessThanOrEquals(e67, e68);
		Expression e182 = new LessThan(e69, e70);
		Expression e183 = new NotEquals(e71, e72);
		Expression e184 = new NotOperator(e183);
		Expression e185 = new LessThanOrEquals(e73, e74);
		Expression e186 = new LessThan(e75, e76);
		Expression e187 = new LikeOperator(e77, e78);
		Expression e188 = new NotOperator(e187);
		Expression e189 = new InOperator(e79, e80);
		Expression e190 = new NotOperator(e189);
		Expression e191 = new LessThanOrEquals(e81, e82);
		Expression e192 = new LessThan(e83, e84);
		Expression e193 = new LikeOperator(e85, e86);
		Expression e194 = new NotOperator(e193);
		Expression e195 = new GreaterThan(e87, e88);
		Expression e196 = new NotOperator(e195);
		Expression e197 = new LessThanOrEquals(e89, e90);
		Expression e198 = new GreaterThan(e91, e92);
		Expression e199 = new NotEquals(e93, e94);
		Expression e200 = new NotOperator(e199);
		Expression e201 = new LessThanOrEquals(e95, e96);
		Expression e202 = new GreaterThan(e97, e98);
		Expression e203 = new LikeOperator(e99, e100);
		Expression e204 = new NotOperator(e203);
		Expression e205 = new InOperator(e101, e102);
		Expression e206 = new NotOperator(e205);
		Expression e207 = new LessThanOrEquals(e103, e104);
		Expression e208 = new GreaterThan(e105, e106);
		Expression e209 = new LikeOperator(e107, e108);
		Expression e210 = new NotOperator(e209);
		Expression e211 = new GreaterThan(e109, e110);
		Expression e212 = new NotOperator(e211);
		Expression e213 = new LessThanOrEquals(e111, e112);
		Expression e214 = new Equals(e113, e114);
		Expression e215 = new NotEquals(e115, e116);
		Expression e216 = new NotOperator(e215);
		Expression e217 = new LessThanOrEquals(e117, e118);
		Expression e218 = new Equals(e119, e120);
		Expression e219 = new LikeOperator(e121, e122);
		Expression e220 = new NotOperator(e219);
		Expression e221 = new InOperator(e123, e124);
		Expression e222 = new NotOperator(e221);
		Expression e223 = new LessThanOrEquals(e125, e126);
		Expression e224 = new Equals(e127, e128);
		Expression e225 = new LikeOperator(e129, e130);
		Expression e226 = new NotOperator(e225);
		Expression e227 = new GreaterThan(e131, e132);
		Expression e228 = new NotOperator(e227);
		List<Expression> list1 = new ArrayList<>();
		list1.add(e133);
		list1.add(e134);
		list1.add(e136);
		Expression e229 = new MultiOrOperator(list1);
		List<Expression> list2 = new ArrayList<>();
		list2.add(e137);
		list2.add(e138);
		list2.add(e140);
		list2.add(e142);
		Expression e230 = new MultiOrOperator(list2);
		List<Expression> list3 = new ArrayList<>();
		list3.add(e143);
		list3.add(e144);
		list3.add(e146);
		list3.add(e148);
		Expression e231 = new MultiOrOperator(list3);
		List<Expression> list4 = new ArrayList<>();
		list4.add(e149);
		list4.add(e150);
		list4.add(e152);
		Expression e232 = new MultiOrOperator(list4);
		List<Expression> list5 = new ArrayList<>();
		list5.add(e153);
		list5.add(e154);
		list5.add(e156);
		list5.add(e158);
		Expression e233 = new MultiOrOperator(list5);
		List<Expression> list6 = new ArrayList<>();
		list6.add(e159);
		list6.add(e160);
		list6.add(e162);
		list6.add(e164);
		Expression e234 = new MultiOrOperator(list6);
		List<Expression> list7 = new ArrayList<>();
		list7.add(e165);
		list7.add(e166);
		list7.add(e168);
		Expression e235 = new MultiOrOperator(list7);
		List<Expression> list8 = new ArrayList<>();
		list8.add(e169);
		list8.add(e170);
		list8.add(e172);
		list8.add(e174);
		Expression e236 = new MultiOrOperator(list8);
		List<Expression> list9 = new ArrayList<>();
		list9.add(e175);
		list9.add(e176);
		list9.add(e178);
		list9.add(e180);
		Expression e237 = new MultiOrOperator(list9);
		List<Expression> list10 = new ArrayList<>();
		list10.add(e181);
		list10.add(e182);
		list10.add(e184);
		Expression e238 = new MultiOrOperator(list10);
		List<Expression> list11 = new ArrayList<>();
		list11.add(e185);
		list11.add(e186);
		list11.add(e188);
		list11.add(e190);
		Expression e239 = new MultiOrOperator(list11);
		List<Expression> list12 = new ArrayList<>();
		list12.add(e191);
		list12.add(e192);
		list12.add(e194);
		list12.add(e196);
		Expression e240 = new MultiOrOperator(list12);
		List<Expression> list13 = new ArrayList<>();
		list13.add(e197);
		list13.add(e198);
		list13.add(e200);
		Expression e241 = new MultiOrOperator(list13);
		List<Expression> list14 = new ArrayList<>();
		list14.add(e201);
		list14.add(e202);
		list14.add(e204);
		list14.add(e206);
		Expression e242 = new MultiOrOperator(list14);
		List<Expression> list15 = new ArrayList<>();
		list15.add(e207);
		list15.add(e208);
		list15.add(e210);
		list15.add(e212);
		Expression e243 = new MultiOrOperator(list15);
		List<Expression> list16 = new ArrayList<>();
		list16.add(e213);
		list16.add(e214);
		list16.add(e216);
		Expression e244 = new MultiOrOperator(list16);
		List<Expression> list17 = new ArrayList<>();
		list17.add(e217);
		list17.add(e218);
		list17.add(e220);
		list17.add(e222);
		Expression e245 = new MultiOrOperator(list17);
		List<Expression> list18 = new ArrayList<>();
		list18.add(e223);
		list18.add(e224);
		list18.add(e226);
		list18.add(e228);
		Expression e246 = new MultiOrOperator(list18);
		List<Expression> list19 = new ArrayList<>();
		list19.add(e229);
		list19.add(e230);
		list19.add(e231);
		list19.add(e232);
		list19.add(e233);
		list19.add(e234);
		list19.add(e235);
		list19.add(e236);
		list19.add(e237);
		list19.add(e238);
		list19.add(e239);
		list19.add(e240);
		list19.add(e241);
		list19.add(e242);
		list19.add(e243);
		list19.add(e244);
		list19.add(e245);
		list19.add(e246);
		steplast3 = new MultiAndOperator(list19);
	}
	
	/**
	 * this method is used to build the step last expression tree 4.
	 */
	private void buildStepLastExpression4() {
		Expression e1 = new ColumnNode("S.D");
		Expression e2 = new DateValue("2017/03/25");
		Expression e3 = new GreaterThan(e1,e2);
		steplast4 = new NotOperator(e3);
	}
	
	/**
	 * this method is used to build the step last expression tree 5.
	 */
	private void buildStepLastExpression5() {
		Expression e1 = new ColumnNode("S.A");
		Expression e2 = new DoubleValue(3.5);
		Expression e3 = new ColumnNode("S.B");
		Expression e4 = new LongValue(4);
		Expression e5 = new ColumnNode("S.C");
		Expression e6 = new TimeValue("12:04:34");
		Expression e7 = new ColumnNode("S.D");
		Expression e8 = new StringValue("sddsds");
		Expression e9 = new GreaterThan(e1,e2);
		Expression e10 = new LessThan(e3,e4);
		Expression e11 = new LikeOperator(e5,e6);
		Expression e12 = new Equals(e7,e8);
		Expression e13 = new NotOperator(e11);
		Expression e14 = new NotOperator(e12);
		List<Expression> list = new ArrayList<>();
		list.add(e9);
		list.add(e10);
		list.add(e13);
		list.add(e14);
		steplast5 = new MultiAndOperator(list);
	}
	
}
