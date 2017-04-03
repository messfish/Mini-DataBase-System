package SmallSQLServer;

import LogicalOperators.CartesianOperators;
import LogicalOperators.DistinctOperators;
import LogicalOperators.GroupByOperators;
import LogicalOperators.HavingOperators;
import LogicalOperators.Operators;
import LogicalOperators.OrderByOperators;
import LogicalOperators.ProjectOperators;
import LogicalOperators.SelectOperators;
import PhysicalOperators.PhysicalVisitor;
import SQLParser.PlainSelect;
import Support.Catalog;

/**
 * This class is mainly used for handling the query, build the logical
 * query plan trees and set up the physical visitors. Call the dump()
 * method from the physical visitor and return.
 * @author messfish
 *
 */
public class QueryHandler {

	/**
	 * This is the method that build the logical query plan and call
	 * the dump() method in the physical visitors.
	 * @param plain the Plain Select object for the query.
	 * @param index the order of the query.
	 * @param catalog the list of schemas available.
	 */
	public static void handle(PlainSelect plain, int index, Catalog catalog) {
		PhysicalVisitor pv = new PhysicalVisitor(plain, catalog);
		Operators ops = BuildQueryPlan(plain);
		ops.accept(pv);
		pv.dump(index);
	}
	
	/**
	 * This method is mainly used for building the logical query plan tree 
	 * by using the information from the Plain Select object. Generally 
	 * speaking, the logical query plan may generally be like this:
	 * The Cartesian operators is usually the leaf node, Followed by a 
	 * Select operators, then we have the Group By and Having operator.
	 * Finally we use the Order by operator and Project operator, and follows
	 * by a distinct operator. Note that besides Cartesian operator, 
	 * every else operators are optional and we only create it when
	 * we need it.
	 * @param plain the object we use to extract valuable information out.
	 * @return the root of the logical query plan tree.
	 */
	private static Operators BuildQueryPlan(PlainSelect plain) {
		Operators result = new CartesianOperators();
		if(plain.getWhereExpression()!=null)
			result = new SelectOperators(result);
		if(plain.getGroupByElements().size()!=0)
			result = new GroupByOperators(result);
		if(plain.getHavingExpression()!=null)
			result = new HavingOperators(result);
		if(plain.getOrderByElements().size()!=0)
			result = new OrderByOperators(result);
		result = new ProjectOperators(result);
		if(plain.isDistinct())
			result = new DistinctOperators(result);
		return result;
	}
	
}
