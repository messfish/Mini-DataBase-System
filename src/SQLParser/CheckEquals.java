package SQLParser;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import SQLExpression.AbstractVisitor;
import SQLExpression.BinaryExpression;
import SQLExpression.Expression;
import SQLExpression.MultipleExpression;
import SQLExpression.UnaryExpression;
import TableElement.Table;

/**
 * As the name suggests, this class helps check whether two trees
 * are identical. That means, they have the same structure. This is
 * mainly used for debugging copied trees to check whether there is
 * a variable that both tree shares. Also, this could be used to check
 * whether the two lists are equal.
 * @author messfish
 *
 */
public class CheckEquals implements AbstractVisitor {

	private boolean isFirst = true; 
	// use this variable to control which queue to push.
	// set true will add elements in queue1.
	// set false will add elements in queue2.
	private Queue<Expression> queue1 = new LinkedList<>();
	private Queue<Expression> queue2 = new LinkedList<>();
	// these two are the queues that handles the BFS.
	
	/**
	 * This method checks whether the two trees are equal. Use BFS
	 * to traverse through the tree and check whether they are the 
	 * same or they are the same object.
	 * @param e1 one of the expression tree
	 * @param e2 one of the expression tree
	 * @return the result that whether they are equal or not.
	 */
	public boolean checkEqual(Expression e1, Expression e2) {
		if(e1==null&&e2==null) return true;
		if(e1==null||e2==null) return false;
		queue1.offer(e1);
		queue2.offer(e2);
		while(!queue1.isEmpty()&&!queue2.isEmpty()) {
			Expression dummy1 = queue1.poll();
			Expression dummy2 = queue2.poll();
			/* they refer to the same object, return false */
			if(dummy1.equals(dummy2)) return false;
			String str1 = dummy1.getString();
			String str2 = dummy2.getString();
			if(!str1.equals(str2)) return false;
			isFirst = true;
			dummy1.accept(this);
			isFirst = false;
			dummy2.accept(this);
		}
		/* if they are the same, there will be no elements left. */
		return queue1.isEmpty() && queue2.isEmpty();
	}
	
	/**
	 * This method is used to check whether two lists of strings are 
	 * equal or not. notice we need to check whether they are null
	 * or not. If not, check whether they have the same elements in the
	 * same order.
	 * @param list1 one of the list we check whether they are equal.
	 * @param list2 one of the list we check whether they are equal.
	 * @return the boolean value shows whether they are equal.
	 */
	public boolean checkEqual(List<String> list1, List<String> list2) {
		if(list1==null&&list2==null) return true;
		if(list1==null||list2==null) return false;
		if(list1.size()!=list2.size()) return false;
		for(int i=0;i<list1.size();i++) {
			String str1 = list1.get(i), str2 = list2.get(i);
			if(!str1.equals(str2))
				return false;
		}
		return true;
	}
	
	/**
	 * This method is used to check whether two lists of expressions are 
	 * equal or not. notice we need to check whether they are null
	 * or not. If not, check whether they have the same elements in the
	 * same order.
	 * @param list1 one of the list we check whether they are equal.
	 * @param list2 one of the list we check whether they are equal.
	 * @return the boolean value shows whether they are equal.
	 */
	public boolean isEqual(List<Expression> list1, List<Expression> list2) {
		if(list1==null&&list2==null) return true;
		if(list1==null||list2==null) return false;
		if(list1.size()!=list2.size()) return false;
		for(int i=0;i<list1.size();i++) {
			Expression str1 = list1.get(i), str2 = list2.get(i);
			if(!checkEqual(str1, str2))
				return false;
		}
		return true;
	}
	
	/**
	 * This method is used to check whether two arrays of integers are 
	 * equal or not. notice we need to check whether they are null
	 * or not. If not, check whether they have the same elements in the
	 * same order.
	 * @param list1 one of the array we check whether they are equal.
	 * @param list2 one of the array we check whether they are equal.
	 * @return the boolean value shows whether they are equal.
	 */
	public boolean checkEqual(int[] list1, int[] list2) {
		if(list1==null&&list2==null) return true;
		if(list1==null||list2==null) return false;
		if(list1.length!=list2.length) return false;
		for(int i=0;i<list1.length;i++) {
			if(list1[i]!=list2[i])
				return false;
		}
		return true;
	}

	/**
	 * This method is used to check whether the two maps are equal. 
	 * Basically it will iterate the first table and check whether the element
	 * is in the second table and the elements they point are equal.
	 * @param map1 one of the map needs to be checked.
	 * @param map2 one of the map needs to be checked.
	 * @return the shows whether the two maps are equal.
	 */
	public boolean checkEqual(Map<String, Table> map1, Map<String, Table> map2) {
		if(map1==null&&map2==null) return true;
		if(map1==null||map2==null) return false;
		if(map1.size()!=map2.size()) return false;
		for(Map.Entry<String, Table> entry : map1.entrySet()) {
			Table table1 = entry.getValue();
			if(!map2.containsKey(entry.getKey()))
				return false;
			if(!map2.get(entry.getKey()).equals(table1))
				return false;
		}
		return true;
	}
	
	/**
	 * This method is used for debugging. Print a list of String with
	 * each element in one line.
	 * @param list the list that needs to be printed.
	 */
	public void printString(List<String> list) {
		for(int i=0;i<list.size();i++)
			System.out.println(list.get(i));
	}
	
	/**
	 * This method is used for debugging. Print a list of Expressions.
	 * There will be a blank line for each expression tree.
	 * @param list the list that needs to be printed. 
	 */
	public void printExpression(List<Expression> list) {
		for(int i=0;i<list.size();i++) {
			StringBuilder sb = new StringBuilder();
			list.get(i).print("", sb);
			System.out.println(sb);
			System.out.println();
		}
	}
	
	/**
	 * This method is used for debugging. Print a array of integer with
	 * each element in one line.
	 * @param list the list that needs to be printed.
	 */
	public void printArray(int[] array) {
		for(int i=0;i<array.length;i++)
			System.out.println(array[i]);
	}
	
	/**
	 * this is the visit method of the expression class. Since only leaf 
	 * expressions will go into this category and there are no
	 * child in these expressions, there is no need to push 
	 * any children in it, simply return it.
	 * @param express the expression that will be visited. 
	 */
	@Override
	public void visit(Expression express) {	}

	/**
	 * this is the visit method of the binary expression class, push the
	 * left child in the queue first and the right child in the queue.
	 * @param express the expression that will be visited. 
	 */
	@Override
	public void visit(BinaryExpression express) {
		if(isFirst) {
			queue1.offer(express.getLeftChild());
			queue1.offer(express.getRightChild());
		}else {
			queue2.offer(express.getLeftChild());
			queue2.offer(express.getRightChild());
		}
	}

	/**
	 * this is the visit method of the unary expression class, push the
	 * only child into the queue.
	 * @param express the expression that will be visited. 
	 */
	@Override
	public void visit(UnaryExpression express) {
		if(isFirst) queue1.offer(express.getChild());
		else queue2.offer(express.getChild());
	}

	/**
	 * this is the visit method of the multiple expression class, push
	 * the children of the class from the left of the list to the right.
	 * @param express the expression that will be visited. 
	 */
	@Override
	public void visit(MultipleExpression express) {
		if(isFirst) {
			for(int i=0;i<express.size();i++)
				queue1.offer(express.getChild(i));
		}else {
			for(int i=0;i<express.size();i++)
				queue2.offer(express.getChild(i));
		}
	}
	
}
