package PhysicalOperators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import LogicalOperators.CartesianOperators;
import LogicalOperators.DistinctOperators;
import LogicalOperators.GroupByOperators;
import LogicalOperators.HavingOperators;
import LogicalOperators.JoinOperators;
import LogicalOperators.OperatorVisitor;
import LogicalOperators.OrderByOperators;
import LogicalOperators.ProjectOperators;
import LogicalOperators.SelectOperators;
import SQLParser.PlainSelect;
import SmallSQLServer.Main;
import Support.Catalog;

/**
 * This class is mainly used for building the query plan tree: It uses the
 * visitor's pattern to traverse through the query tree.
 * @author messfish
 *
 */
public class PhysicalVisitor implements OperatorVisitor {

	private PlainSelect ps;
	private Operator op;
	private Catalog catalog;
	
	/**
	 * Constructor: this constructor is used to pass the argument 
	 * to the global variable.
	 * @param ps the select query.
	 * @param catalog the list of schemas available.
	 */
	public PhysicalVisitor(PlainSelect ps, Catalog catalog) {
		this.ps = ps;
		this.catalog = catalog;
	}
	
	/**
	 * This is the visiting method of the order by Operators.
	 * @param cart the logical Order By Operators that needs to be visited.
	 */
	@Override
	public void visit(OrderByOperators order) {
		order.getChild().accept(this);
		op = new SortOperator(op, ps.getOrderByElements(), ps.isDescList());
	}

	@Override
	public void visit(GroupByOperators group) {
		
	}

	/**
	 * This is the visiting method of the Project Operators.
	 * @param cart the logical Project Operators that needs to be visited.
	 */
	@Override
	public void visit(ProjectOperators project) {
		project.getChild().accept(this);
		op = new ProjectOperator(op, ps.getSelectElements(), ps.getSelectAlias());
	}

	/**
	 * This is the visiting method of the Distinct Operators.
	 * @param cart the logical Distinct Operators that needs to be visited.
	 */
	@Override
	public void visit(DistinctOperators distinct) {
		distinct.getChild().accept(this);
		op = new DistinctOperator(op);
	}

	@Override
	public void visit(JoinOperators join) {
		
	}

	@Override
	public void visit(HavingOperators having) {
		
	}

	/**
	 * This is the visiting method of the Select Operators.
	 * @param cart the logical Select Operators that needs to be visited.
	 */
	@Override
	public void visit(SelectOperators select) {
		select.getChild().accept(this);
		op = new SelectOperator(op, ps.getWhereExpression());
	}

	/**
	 * This is the visiting method of the Cartesian Operators.
	 * @param cart the logical Cartesian Operators that needs to be visited.
	 */
	@Override
	public void visit(CartesianOperators cart) {
		op = new CartesianOperator(ps.getFromList(), catalog);
	}
	
	/**
	 * This method is used to get all the tuples available and print all
	 * the valid tuples out in the ordered format.
	 * @param index the index of the query.
	 */
	public void dump(int index) {
		int[] datasize = op.dump(index);
		int numofplus = datasize.length + 1;
		for(int i=0;i<datasize.length;i++) {
			datasize[i] += 2;
			numofplus += datasize[i]; 
		}
		System.out.println();
		File file = new File(Main.getOutput() + "/" + index);
		try {
			BufferedReader buff = new BufferedReader(new FileReader(file));
			String s = buff.readLine();
			printHead(s, numofplus, datasize);
			while((s=buff.readLine())!=null)
				printTuple(s, datasize);
			for(int i=0;i<numofplus;i++) 
				System.out.print("+");
			System.out.println();
			System.out.println();
			buff.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This method is used to print the head of the table. Note all the 
	 * table elements should be in the middle of printed form.
	 * @param s the string that stores the list of attribute names.
	 * @param numofplus the number of pluses to be printed.
	 * @param datasize the array shows the size of each attribute.
	 */
	private void printHead(String s, int numofplus, int[] datasize) {
		for(int i=0;i<numofplus;i++) 
			System.out.print("+");
		System.out.println();
		String[] titlearray = s.split("\\s+");
		System.out.print("|");
		for(int i=0;i<datasize.length;i++) 
			printColumn(datasize, titlearray, i);
		System.out.println();
		for(int i=0;i<numofplus;i++) 
			System.out.print("+");
		System.out.println();
	}
	
	/**
	 * This method is used to print the tuple out. 
	 * @param s the string that stores a list of data.
	 * @param datasize the array that stores the size of the data.
	 */
	private void printTuple(String s, int[] datasize) {
		String[] store = new String[datasize.length];
		int index = 0, arraypoint = 0;
		while(s.charAt(index)!=' ')
			index++;
		index++;
		while(index<s.length()) {
			int datalength = 0;
			while(s.charAt(index)!='/') {
				datalength = datalength * 10 + (int)(s.charAt(index) - '0');
				index++;
			}
			index++;
			store[arraypoint] = s.substring(index, index + datalength);
			arraypoint++;
			index += datalength + 1;
		}
		System.out.print("|");
		for(int i=0;i<datasize.length;i++) 
			printColumn(datasize, store, i);
		System.out.println();
	}
	
	/**
	 * This method is used to print a single attribute. It will try to place
	 * the data in the middle of the space.
	 * @param datasize the array stores the size of each attribute.
	 * @param dataarray the array stores the data of the whole tuple.
	 * @param index the index that we use to extract the data.
	 */
	private void printColumn(int[] datasize, String[] dataarray, int index) {
		int left = (datasize[index] - dataarray[index].length()) / 2,
			right = datasize[index] - left - dataarray[index].length();
		for(int j=0;j<left;j++)
			System.out.print(" ");
		System.out.print(dataarray[index]);
		for(int j=0;j<right;j++)
			System.out.print(" ");
		System.out.print("|");
	}

}
