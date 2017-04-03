package BPlusTree;

import java.util.List;

import TableElement.DataType;

/**
 * This class is used to print the B+ Tree.
 * @author messfish
 *
 */
public class Printer {
	
	/**
	 * this method is used for debugging. It will print the B+ Tree.
	 * Basically it will perform the pre-order traverse of the tree.
	 * @param tree the B+ Tree that will be printed.
	 * @return the string that represents the pre-order traverse of the tree.
	 */
	public String printTree(BPlusTree tree) {
		StringBuilder sb = new StringBuilder();
		print(tree.getLocation(),"",sb, tree.getHeight());
		return sb.toString();
	}
	
	/**
	 * This method traverse the tree and store the pre-order traverse to
	 * the result.
	 * @param filelocation the location of the file.
	 * @param start it is used to indicate the level of the B
	 * @param sb the String Builder that stores the data.
	 * @param height the height of the tree.
	 */
	private void print(String filelocation, String start
						, StringBuilder sb, int height) {
		Node treenode = null;
		/* remember all the leaf nodes are at the bottom, so if the length
		 * of the start equals to the height, it means this is a leaf node. */
		if(start.length()==height) {
			treenode = new LeafNode(filelocation);
			sb.append(start).append("This is a leaf node:").append("\n");
			List<DataType[]> keylist = treenode.keylist;
			List<List<int[]>> valuelist = ((LeafNode)treenode).valuelist;
			for(int i=0;i<keylist.size();i++) {
				StringBuilder temp = new StringBuilder();
				temp.append(start).append("Key: ");
				DataType[] datalist = keylist.get(i);
				for(int j=0;j<datalist.length;j++)
					temp.append(datalist[j].print()).append(" ");
				temp.append("Values: ");
				for(int[] tupleindex : valuelist.get(i))
					temp.append("[").append(tupleindex[0]).append(",").
						append(tupleindex[1]).append("],");
				sb.append(temp.deleteCharAt(temp.length() - 1)).append("\n");
			}
		}else {
			treenode = new IndexNode(filelocation);
			sb.append(start).append("This is an index node:").append("\n");
			List<DataType[]> keylist = treenode.keylist;
			sb.append(start).append("Keys: ");
			for(DataType[] datalist : keylist) {
				sb.append("[");
				for(DataType data : datalist) 
					sb.append(data.print()).append(",");
				sb.deleteCharAt(sb.length() - 1).append("] ");
			}
			sb.deleteCharAt(sb.length() - 1).append("\n");
			for(int i=0;i<=keylist.size();i++)
				print(filelocation+i+"/",start+"-",sb,height);
		}
	}
	
}
