package SQLParser;

import java.util.HashSet;
import java.util.Set;

import SQLExpression.AllOperator;
import SQLExpression.AndOperator;
import SQLExpression.AnyOperator;
import SQLExpression.Equals;
import SQLExpression.ExistsOperator;
import SQLExpression.Expression;
import SQLExpression.GreaterThan;
import SQLExpression.GreaterThanOrEquals;
import SQLExpression.InOperator;
import SQLExpression.LessThan;
import SQLExpression.LessThanOrEquals;
import SQLExpression.LikeOperator;
import SQLExpression.NotEquals;
import SQLExpression.NotOperator;
import SQLExpression.OrOperator;
import SQLExpression.Parenthesis;
import SQLExpression.Subselect;

/**
 * This class handles the having clause by converting the clause
 * into an expression tree. Please note it has nothing to do with
 * checking whether the clause is valid or not. So it could only 
 * be dealt with valid language.
 * 
 * Here is the grammar of the parsing:
 * E := Co {{AND|OR} Co}*
 * Co:= NOT Co | NOT (E) | (E) | Atom
 * Atom := Ca {{<|>|<=|>=|=|<>|LIKE}Ca} 
 * 
 * The E stands for expression, this is usually constructed by
 * a list of comparison linked with "AND" or "OR". 
 * 
 * The Co stands for comparison, this is usually constructed by
 * having a "NOT" expression connecting a sub expression in a 
 * parenthesis or just a comparison. Also it could be a sub 
 * expression in a parenthesis or the most fundamental one: the 
 * comparison between two calculations or the query followed by 
 * a sub expression. 
 * 
 * @author messfish
 *
 */
public class LogicalExpressionParser {

	private String[] tokens;
	private int index; 
	// the index points to the current element which will be parsed.
	private Set<String> stopset;
	// the set of string segments that the index needs to stop at this point.
	private Set<String> subqueryset;
	// the set of subquery that the index needs to stop at this point.
	// usually it indicates there is a sub query.
	
	/**
	 * Constructor: this constructor takes an array of string as the 
	 * parameter and set the starting point to 0.
	 * @param input
	 */
	public LogicalExpressionParser(String[] input) {
		tokens = input;
		index = 0;
		String[] stoplist = {"<",">","<=",">=","=","<>","NOT",
				"IN","LIKE","ANY","ALL","EXISTS"};
		stopset = new HashSet<>();
		for(String str : stoplist)
			stopset.add(str);
		String[] subquerylist = {"NOT", "IN", "ANY", "ALL", "EXISTS"};
		subqueryset = new HashSet<>();
		for(String str : subquerylist)
			subqueryset.add(str);
	}
	
	/**
	 * Parse the expression and but the expression tree.
	 * @return the root node of the expression tree.
	 */
	public Expression parse() {
		return express();
	}
	
	/**
	 * This method build the string from the start point to the 
	 * end point, notice that it includes the start point but not
	 * the end point. Also there is an empty space between each tokens.
	 * @param start the starting point of the token list.
	 * @param endpoint the ending point of the token list.
	 * @return the string that indicates the query.
	 */
	private String BuildString(int start, int endpoint) {
		StringBuilder sb = new StringBuilder();
		while(start < endpoint) {
			sb.append(tokens[start]).append(" ");
			start++;
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}
	
	/**
	 * This is the class that gets the root value of the expression.
	 * @param connection the sub query words in a list.
	 * @param pointer the number of sub query words in a list.
	 * @param left the left expression sub tree.
	 * @param right the right expression sub tree.
	 * @return the root of the expression tree.
	 */
	private Expression buildSub(String[] connection, int pointer,
			                    Expression left, Expression right) {
		/* the pointer could be either 1 or 2, so we execute the code
		 * when the value of the pointer is 2. */
		Expression result = null;
		if(pointer==2) {
			if(connection[1].equals("IN")) 
				result = new InOperator(left, right);
			else if(connection[1].equals("ALL"))
				right = new AllOperator(right);
			else if(connection[1].equals("ANY"))
				right = new AnyOperator(right);
		}
		/* now build the result based on the first element of the array. */
		if(connection[0].equals("<")) 
			result = new LessThan(left,right);
		else if(connection[0].equals("<="))
			result = new LessThanOrEquals(left,right);
		else if(connection[0].equals(">")) 
			result = new GreaterThan(left,right);
		else if(connection[0].equals(">=")) 
			result = new GreaterThanOrEquals(left,right);
		else if(connection[0].equals("=")) 
			result = new Equals(left,right);
		else if(connection[0].equals("<>")) 
			result = new NotEquals(left,right);
		else if(connection[0].equals("NOT"))
			result = new NotOperator(result);
		else if(connection[0].equals("IN"))
			result = new InOperator(left, right);
		return result;
	}
	
	/**
	 * This method is used to deal with the language that has a subquery
	 * words in it. It will return an expression tree. Notice there will
	 * be an expression that is Not Like. Be sure to take care of this
	 * special case since there it will not involve an sub query.
	 * @param endpoint the integer that the index will end at.
	 * @return an expression tree where the subquery words or "not" or
	 * the comparison expression could be the root.
	 */
	private Expression subquery(int endpoint) {
		Expression result = null;
		int start = index;
		/* if the first token equals with "EXISTS", that means
		 * this is an exist sub query. */
		if(tokens[start].equals("EXISTS")){
			String subquery = BuildString(start+2, endpoint-1);
			Expression sub = new Subselect(subquery);
			Expression parenthesis = new Parenthesis(sub);
			result = new ExistsOperator(parenthesis);
		}
		/* else if the first token equals with "NOT", that means
		 * this is an "NOT EXIST". */
		else if(tokens[start].equals("NOT")){
			String subquery = BuildString(start+3, endpoint-1);
			Expression sub = new Subselect(subquery);
			Expression parenthesis = new Parenthesis(sub);
			result = new ExistsOperator(parenthesis);
			result = new NotOperator(result);
		}
		/* this will handle the rest of the cases. */
		else{
			/* this is the calculation of the left part of the comparison.
			 * Notice the index ends at the string which is included in 
			 * the set above. */
			while(index<tokens.length&&!stopset.contains(tokens[index]))  
			    index++;
			CalculationParser lcal = new CalculationParser(tokens,start,index);
			Expression left = lcal.parse();
			/* use an array of string to store those sub query words. */
			String[] connection = new String[2];
			int pointer = 0;
			while(index<tokens.length&&stopset.contains(tokens[index]))
				connection[pointer++] = tokens[index++];
			/* handle the "not like" case at here. */
			if(pointer>1&&connection[1].equals("LIKE")) {
				CalculationParser rcal = 
						new CalculationParser(tokens,index,endpoint);
				Expression right = rcal.parse();
				Expression like = new LikeOperator(left, right);
				result = new NotOperator(like);
			}else{
				/* get the sub query into the right expression tree. 
				 * Note the current index is the left parenthesis, so
				 * increment the index by 1. */
				String subquery = BuildString(index + 1, endpoint-1);
				Expression sub = new Subselect(subquery);
				Expression right = new Parenthesis(sub);
				/* use this method to retrieve the root back. */
				result = buildSub(connection, pointer, left, right);
				// don't forget to put the index at the next valid point!
				index = endpoint; 
			}
		}
		return result;
	}
	
	/**
	 * This method is used to deal with the comparison between two calculations,
	 * It return the expression tree whose root is the comparison node. Notice
	 * this method will not deal with the method that has the language like "NOT"
	 * "ANY", "ALL", "IN", it will be handled by different method.
	 * @param the integer that the index will be end at.
	 * @return the comparison expression as the root of the root.
	 */
	private Expression calculation(int endpoint) {
		Expression result = null;
		int start = index;
		/* this is the calculation of the left part of the comparison.
		 * Notice the index ends at the string which is included in 
		 * the set above. */
		while(index<tokens.length&&!stopset.contains(tokens[index]))  
		    index++;
		CalculationParser lcal = new CalculationParser(tokens,start,index);
		Expression left = lcal.parse();
		int dummy = index; // mark the index of the comparison operator.
		CalculationParser rcal = new CalculationParser(tokens,dummy+1,endpoint);
		Expression right = rcal.parse();
		/* this part mainly handles the seven different comparisons shown above.
		 * construct the comparison node with the left and right calculation
		 * expressions which are shown above. */
		if(tokens[dummy].equals("<")) 
			result = new LessThan(left,right);
		else if(tokens[dummy].equals("<="))
			result = new LessThanOrEquals(left,right);
		else if(tokens[dummy].equals(">")) 
			result = new GreaterThan(left,right);
		else if(tokens[dummy].equals(">=")) 
			result = new GreaterThanOrEquals(left,right);
		else if(tokens[dummy].equals("=")) 
			result = new Equals(left,right);
		else if(tokens[dummy].equals("<>")) 
			result = new NotEquals(left,right);
		else if(tokens[dummy].equals("LIKE"))
			result = new LikeOperator(left,right);
		index = endpoint; 
		// At last, do not forget to assign the index to endpoint!
		return result;
	}
	
	/**
	 * Parse the remaining input as far as needed to get the next comparison.
	 * @return the (root node of) the resulting subtree
	 */
	private Expression comparison() {
		Expression result = null;
		/* Here is the conditions of the comparison.
		 * The first one starts at the "(", it indicates the start of (E). 
		 * Get the result from the expression and store the result into
		 * a parenthesis expression, return that expression.*/
		if(index<tokens.length&&tokens[index].equals("(")){
			index++;
			Expression express = express();
			result = new Parenthesis(express);
		}
		/* this part handles the case when a "NOT" language exists. 
		 * Just increment the index and do the recursion. Notice if 
		 * the next part is (E), it could be handled by the code
		 * above, if the next part is a comparison, doing the recursion
		 * will be fine.*/
		else if(index<tokens.length&&tokens[index].equals("NOT")){
			index++;
			Expression temp = comparison();
			result = new NotOperator(temp);
		}
		/* The last part indicates a normal comparison between to
		 * calculations. It will iterate the atomic part to check whether
		 * there are the string that belongs to the sub query part. If yes,
		 * use the subquery() method to handle, if not, use the calculation().
		 * Since that part is rather long, I will use two methods to handle.*/
		else{
			int dummy = index, numofparenthesis = 0;
			boolean isSub = false;
			/* jump out of the while loop when the number of parenthesis is 
			 * negative or we reach "AND" or "OR". */
			while(dummy < tokens.length
					&&!tokens[dummy].equals("AND")&&!tokens[dummy].equals("OR")){
				if(tokens[dummy].equals("(")) numofparenthesis++;
				if(tokens[dummy].equals(")")) numofparenthesis--;
				if(numofparenthesis < 0) break;
				if(subqueryset.contains(tokens[dummy])) 
					isSub = true;
				dummy++;
			}
			if(!isSub) result = calculation(dummy);
			else result = subquery(dummy);
		}
		return result;
	}
	
	/**
	 * Parse the remaining input as far as needed to get the next expression.
	 * @return the (root node of) the resulting subtree
	 */
	private Expression express() {
		/* get the left part of the expression */
		Expression result = comparison(), temp = null;
		/* jump out of the while loop when we meet the end of the array
		 * or when the index points at string ")" */
		while(index<tokens.length&&!tokens[index].equals(")")
				&&(tokens[index].equals("AND")||tokens[index].equals("OR"))){
			int dummy = index;
			index++;
			/* get the right part of the expression */
			Expression result2 = comparison();
			if(tokens[dummy].equals("AND"))
				temp = new AndOperator(result, result2);
			else temp = new OrOperator(result, result2);
			result = temp; // set the root to the returning expression.
		}
		/* notice that if the index is pointing this part: ")", we need to 
		 * move the index forward since this indicates the end of (E). */
		if(index<tokens.length&&tokens[index].equals(")"))
			index++;
		return result;
	}
	
}

