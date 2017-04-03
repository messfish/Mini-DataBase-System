package SQLExpression;

/**
 * This is the second-level class for the expression class hierarchy.
 * It manages to handle the only child.
 * @author messfish
 *
 */
public abstract class UnaryExpression extends Expression {

	private Expression child;
	
	/**
	 * Constructor: this constructor takes the child pointer as
	 * parameter and store that in the global variable.
	 * @param child the child pointer mentioned above.
	 */
	public UnaryExpression(Expression child) {
		this.child = child;
	}
	
	/**
	 * this is the getter method of the child.
	 * @return the child pointer.
	 */
	public Expression getChild(){
		return child;
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
