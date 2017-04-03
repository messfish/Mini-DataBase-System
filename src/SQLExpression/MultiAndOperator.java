package SQLExpression;

import java.util.List;

/**
 * this is the class that just like and operator but it will have
 * multiple children. Note that this operator will not be included
 * in the expression visitor so I do not implement the accept method.
 *
 * @author messfish
 *
 */
public class MultiAndOperator extends MultipleExpression{

	/**
	 * Constructor: this constructor extends the multiple 
	 * expression constructor and do what the super class do.
	 * @param childlist a list of child operators.
	 */
	public MultiAndOperator(List<Expression> childlist) {
		super(childlist);
	}

	/**
	 * Since we do not include this class for the expression visitor,
	 * I will just leave this method blank.
	 * @param expression expression the visitor to be accepted.
	 */
	@Override
	public void accept(ExpressionVisitor expression) {
		expression.visit(this);
	}
	
	/**
	 * this is mainly used for debugging, it prints out a string to
	 * identify the type of the expression.
	 */
	@Override
	public String getString() {
		String result = "and " + size();
		return result;
	}
	
	/**
	 * this method is the method for accepting visitor: just calls the 
	 * visitor in the CloneExpressionVisitor Interface. The logic of the tree
	 * traverse will be handled by the class who implement that interface.
	 * @param expression the visitor to be accepted.
	 */
	@Override
	public Expression accept(CloneExpressionVisitor expression) {
		return expression.visit(this);
	}

	/**
	 * This is mainly used for debugging: it will print the 
	 * structure of the expression in a tree structure. The 
	 * number of "-" indicates the level of the tree.
	 * @param s a list of '-' to indicate the level of the tree.
	 * @param sb the string that will be used to generate the output.
	 */
	@Override
	public void print(String s, StringBuilder sb) {
		sb.append(s+"[And]").append("\n");
		for(int i=0;i<size();i++)
			getChild(i).print(s+"-", sb);
	}

	/**
	 * Since this Expression is not a binary expression anymore. I will
	 * also leave this method as blank.
	 * @param sb the string builder that handles the output of the method.
	 */
	@Override
	public void toString(StringBuilder sb) {
		
	}

}
