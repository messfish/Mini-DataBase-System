package SQLExpression;

import java.util.List;

/**
 * this class is mainly used for grouping multiple things 
 * in a single expression. It is basically the parent of 
 * multiple and operator and multiple or operator.
 * @author messfish
 *
 */
public abstract class MultipleExpression extends Expression{

	private List<Expression> childlist;
	
	/**
	 * Constructor: this constructor takes a list of 
	 * child as parameter and put that to the global variable.
	 * @param childlist a list of child expressions.
	 */
	public MultipleExpression(List<Expression> childlist) {
		this.childlist = childlist;
	}
	
	/**
	 * this method returns the size of the list of expressions.
	 * @return the size of the list.
	 */
	public int size() {
		return childlist.size();
	}
	
	/**
	 * this is the getter method of the expression list.
	 * @return the list of expression.
	 */
	public List<Expression> getList() {
		return childlist;
	}
	
	/**
	 * this method returns the child in the expression list given the index.
	 * @param index the index of the element in the list.
	 * @return the expression located in that index.
	 */
	public Expression getChild(int index) {
		return childlist.get(index);
	}
	
	/**
	 * this method removes the child from the expression list given the index.
	 * @param index the index of the element in the list.
	 * @return the expression located in that index.
	 */
	public Expression removeChild(int index) {
		return childlist.remove(index);
	}
	
	/**
	 * this is the setter method of the function.
	 * @param index the index to set the expression.
	 * @param express the expression that will be settled.
	 */
	public void setChild(int index, Expression express) {
		childlist.set(index, express);
	}
	
	/**
	 * this function gets the index of a specific expression.
	 * @param express the expression we want to find.
	 * @return the index where the expression locate.
	 */
	public int getIndex(Expression express) {
		return childlist.indexOf(express);
	}
	
	/**
	 * this method adds an expression in the specific index.
	 * @param index the index to put the expression in.
	 * @param express the expression that will be inserted.
	 */
	public void addChild(int index, Expression express) {
		childlist.add(index, express);
	}
	
	/**
	 * this method is the method for accepting visitor: just calls the 
	 * visitor in the CloneExpressionVisitor Interface. The logic of the tree
	 * traverse will be handled by the class who implement that interface.
	 * @param expression the visitor to be accepted.
	 */
	public void accept(AbstractVisitor visitor) {
		visitor.visit(this);
	}
		
	/**
	 * This method returns whether the node is a leaf node. At this time,
	 * we return false because all the expression class extends this class
	 * are not leaf nodes.
	 * @return a value shows whether this expression is a leaf node or not.
	 */
	@Override
	public boolean isLeaf() {
		return false;
	}
	
}
