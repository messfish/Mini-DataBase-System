package SQLParser;

import SQLExpression.AddOperator;
import SQLExpression.ColumnNode;
import SQLExpression.DateValue;
import SQLExpression.DivideOperator;
import SQLExpression.DoubleValue;
import SQLExpression.Expression;
import SQLExpression.LongValue;
import SQLExpression.MinusOperator;
import SQLExpression.MultiplyOperator;
import SQLExpression.NegativeValue;
import SQLExpression.Parenthesis;
import SQLExpression.StringValue;
import SQLExpression.TimeValue;

/**
 * This is the class that could parse the calculation string and return
 * an expression tree. Notice this does nothing to do with grammar 
 * checking so it is only supposed to work on correct input.
 * 
 * Here is the logic on how to perform the parsing:
 * 
 * The core of every parser is a grammar. This is a set of rules that
 * explains the structure of the intermediate representation.
 * For this parser, I use this following grammar:
 * 
 * E: = T {{+|-} T}*
 * T: = F {{*|/} F}*
 * F: = (E) | - F | number | string
 * 
 * The symbols E, T and F are called nonterminals; They stand for expression,
 * term and factor respectively. Those remaining marks like +, -, *, /, number
 * and the string represents the terminals. 
 * 
 * The grammar above could reflect the code that I wrote:
 * In the factor part, I divide the execution into three parts:
 * The first part is an expression in a parenthesis, I detect this by spotting
 * a left parenthesis.
 * The second part is a negative sign with a factor attach to it.
 * I detect this by checking a minus sign.
 * The remaining part is the string or number. I distinguish them by checking
 * the type to see it is a number or not.
 * 
 * In the term part, I check whether we reach the end of the string list or
 * the right parenthesis ( Note that there is a (E) in the factor which means
 * the right parenthesis also indicates the end of the term.) and continue until
 * the factor is not * or /. 
 * 
 * The mechanism of the expression part should be the same, except this time.
 * we need to move the index forward when we detect the right parenthesis, 
 * otherwise the index will stuck at that point.
 * 
 * @author messfish
 *
 */
public class CalculationParser {

	private String[] segments;
	private int index; // the pointer to the segment that will be processed.
	private int endpoint; // the pointer indicates where the index should stop.
	
	/**
	 * Constructor: this constructor takes an array of string as parameters
	 * and set it to the global variable. And set the index to the starting 
	 * point.
	 * @param input an array of string which will be parsed.
	 */
	public CalculationParser(String[] input) {
		segments = input;
		index = 0;
		endpoint = input.length;
	}
	
	/**
	 * Constructor: another constructor which has the starting point
	 * and the ending point as two parameters. 
	 * @param input an array of string which will be parsed.
	 * @param start the starting point of the array.
	 * @param end the ending point of the array. Notice the index will
	 * not reach it. 
	 */
	public CalculationParser(String[] input, int start, int end) {
		segments = input;
		index = start;
		endpoint = end;
	}

	/**
	 * Parse the input and build the expression tree.
	 * @return the root node of the expression tree.
	 */
	public Expression parse() {
		return expression();
	}
	
	/**
	 * This method checks whether the string is a string value or
	 * not. The string value should be any content that is surrounded
	 * by double quotes.
	 * @param s the string that will be checked.
	 * @return the boolean value shows whether the string is string or not.
	 */
	private boolean isString(String s) {
		if(s.length()<2) return false;
		return s.charAt(0)=='"'&&s.charAt(s.length()-1)=='"';
	}
	
	/**
	 * This method checks whether the string is a time value or not.
	 * Normally the format of the time value should be "%%:%%:%%", the 
	 * first one is the hour, the second one is the minute and the last
	 * one is the second.
	 * @param s the string that will be checked.
	 * @return the boolean value shows whether the string is time or not.
	 */
	private boolean isTime(String s) {
		/* Note the first and the last index of the string is the double
		 * quote. So we should leave them out. */
		if(s.length()<2) return false;
		String[] time = s.substring(1,s.length()-1).split(":");
		if(time.length!=3)
			return false;
		int[] parameters = {24,60,60};
		for(int i=0;i<3;i++) {
			if(time[i].length()!=2||!isLong(time[i]))
				return false;
			long data = Long.parseLong(time[i]);
			if(data>=parameters[i])
				return false;
		}
		return true;
	}
	
	/**
	 * This method checks whether the string is a date value or not.
	 * Normally the format of the date should be "%%%%/%%/%%", the first
	 * one is the date, the second one is the month and the last one is 
	 * the day.
	 * @param s the string that will be checked.
	 * @return the answer "The string is a long value."
	 */
	private boolean isDate(String s) {
		if(s.length()<2) return false;
		int[] range = {31,28,31,30,31,30,31,31,30,31,30,31};
		int index = 0;
		/* Note the first and the last index of the string is the double
		 * quote. So we should leave them out. */
		String[] date = s.substring(1, s.length() - 1).split("/");
		if(date.length!=3) 
			return false;
		if(date[0].length()!=4||!isLong(date[0]))
			return false;
		long data = Long.parseLong(date[0]);
		/* this indicates the date is a lunar year. */
		if(data%400==0||(data%100!=0&&data%4==0))
			range[1]++;
		if(date[1].length()!=2||!isLong(date[1]))
			return false;
		data = Long.parseLong(date[1]);
		if(data>12) 
			return false;
		index = (int) (data - 1);
		if(date[2].length()!=2||!isLong(date[2]))
			return false;
		data = Long.parseLong(date[2]);
		if(data>range[index]) 
			return false;
		return true;
	}
	
	/**
	 * This method checks whether the string is a long value or not.
	 * @param s the string that will be checked.
	 * @return the answer "The string is a long value."
	 */
	private boolean isLong(String s) {
		try{
			Long.parseLong(s);
		}catch(Exception e) {
			return false;
		}
		return true;
	}
	
	/**
	 * This method checks whether the string is a double value or not.
	 * @param s the string that will be checked.
	 * @return the answer "The string is a double value."
	 */
	private boolean isDouble(String s) {
		try{
			Double.parseDouble(s);
		}catch(Exception e){
			return false;
		}
		return true;
	}
	
	/**
	 * Parse the remaining input as far as needed to get the next factor
	 * @return the (root node of) the resulting subtree
	 */
	private Expression factor() {
		Expression result = null;
		// if the token is "(", it means this is the start of (E).
		// get the result from the expression() and store the 
		// result in a parenthesis expression. Return that expression.
		if(index<endpoint&&segments[index].equals("(")){
			index++;
			Expression express = expression();
			Expression parenthesis = new Parenthesis(express);
		    return parenthesis;
		}
		// if the token is "-", it means this is an unary minus factor.
		else if(index<endpoint&&segments[index].equals("-")){
			index++;
			Expression dummy = new NegativeValue(factor());
		    return dummy;
		}
		/* the string could be one of these queries, handle them seperately.
		 * Double Long Time Date Column String. */
		else if(index<endpoint){
			if(isLong(segments[index])) {
				long dummy = Long.parseLong(segments[index]);
				result = new LongValue(dummy);
			}else if(isDouble(segments[index])){
				double dummy = Double.parseDouble(segments[index]);
				result = new DoubleValue(dummy);
			}else if(isDate(segments[index])){
				result = new DateValue(segments[index]);
			}else if(isTime(segments[index])){
				result = new TimeValue(segments[index]);
			}else if(isString(segments[index])){
				result = new StringValue(segments[index]);
			}else result = new ColumnNode(segments[index]);
		    index++;
		}
		return result;
	}

	/**
	 * Parse the remaining input as far as needed to get the next term
	 * @return the (root node of) the resulting subtree
	 */
	private Expression term() {
		Expression result = factor(), temp = null;
		// jump out when we meet the end of the array or when the 
        // current token is ")".
		while(index<endpoint&&!segments[index].equals(")")
             &&(segments[index].equals("*")||segments[index].equals("/"))){
        	    int dummy = index;
        	    index++;
        	    Expression result2 = factor();
        	    if(segments[dummy].equals("*")) 
        		    temp = new MultiplyOperator(result,result2);
        	    else temp = new DivideOperator(result,result2);
        	    result = temp; // set the root to the return Expression.
        }
		return result;
		
	}

	/**
	 * Parse the remaining input as far as needed to get the next expression
	 * @return the (root node of) the resulting subtree
	 */
	private Expression expression() {
		Expression result = term(), temp = null;
        // jump out when we meet the end of the array or when the 
        // current token is ")".
        while(index<endpoint&&!segments[index].equals(")")
            &&(segments[index].equals("+")||segments[index].equals("-"))){
        	    int dummy = index;
        	    index++;
        	    Expression result2 = term();
        	    if(segments[dummy].equals("+")) 
        		    temp = new AddOperator(result,result2);
        	    else temp = new MinusOperator(result,result2);
        	    result = temp; // set the root to the return Expression.
        }
        // when the current token is ")", it means this is the end of (E).
        // therefore, we should move the pointer forward.
        if(index<endpoint&&segments[index].equals(")"))
        	index++;
		return result;
	}
	
}
