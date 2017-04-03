package BPlusTree;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import TableElement.DataType;

/**
 * This is the class that is the actual B+Tree. Note there are some
 * assumptions about the tree: The order of the tree should not be 
 * larger than the number of keys but should not be smaller than the
 * number of keys if you multiply it by 2. We use the alternative (3)
 * for the method to store the keys.
 * @author messfish
 *
 */
public class BPlusTree {

	protected static final int KEY_LIMITS = 100;
	private static final int NUM_OF_BYTES = 4096;
	private Compare compare = new Compare();
	private int height;
	private String location;
	private Node root;
	
	/**
	 * Constructor: this constructor is used to build the B+ Tree
	 * by using the argument as the file location of the root.
	 * @param filelocation
	 */
	public BPlusTree(String filelocation) {
		location = filelocation;
		root = new LeafNode(location + "/keylist");
	}
	
	/**
	 * This method is used to get the height of the B+ Tree. Note since
	 * all the leaf nodes are in the bottom level of the B+ Tree, so 
	 * this could be a good indicator of whether the current node is
	 * an index node or a leaf node.
	 * @return the height of the tree.
	 */
	public int getHeight() {
		return height;
	}
	
	/**
	 * This method is used to get the file location of the root node.
	 * @return the location of the root node.
	 */
	public String getLocation() {
		return location;
	}
	
	/**
	 * Search the value for a specific key, it will return a list
	 * of table entries. Return null if the key is not found.
	 * @param key the key that will be searched.
	 * @return the list of table entries, null means the key does not found.
	 */
	public List<int[]> search(DataType[] key) {
		Node temp = root;
		return search(temp,key,0);
	}

	/**
	 * This helper function is used to return a list of page entries.
	 * It will go through the B+ Tree recursively to get the result.
	 * @param temp the node that is used for searching.
	 * @param key the key that will be searched.
	 * @param depth the depth of the B+ Tree.
	 * @return the list of table entries, null means the key does not found.
	 */
	private List<int[]> search(Node temp, DataType[] key, int depth) {
		if(temp==null) return null;
		if(depth == height){
			int index = searchIndex(temp, key);
			if(index!=-1) {
				if(key.length==temp.numofkeys)
					return ((LeafNode)temp).valuelist.get(index);
				/* this is the case where the key array is the prefix of
				 * the key of B+ Tree. In this case, we need to traverse
				 * the tree from leaf node to leaf node. */
				else {
					Node dummy = temp;
					List<int[]> result = new ArrayList<>();
					while(dummy!=null) {
						int endindex = searchEnd(dummy,index,key,result);
						if(endindex==dummy.keylist.size())
							dummy = getNextNode(dummy.filelocation, height);
						else break;
					}
					return result;
				}
			}
			return null;
		}
		else{
			IndexNode node = (IndexNode) temp;
		    if(compare.compareArray(key, temp.keylist.get(0))<0) 
		    	return search(node.getChild(0,depth+1==height),key,depth+1);
		    else if(compare.compareArray(key, temp.keylist.get(temp.keylist.size()-1))>=0)
		    	return search(node.getChild(node.keylist.size(), 
		    					depth+1==height),key,depth+1);
		    else{
		    	int index = 0;
		    	for(int i=0;i<temp.keylist.size()-1;i++){
		    		if(compare.compareArray(key, temp.keylist.get(i))>=0
		    				&&compare.compareArray(key, temp.keylist.get(i+1))<0){
		    			index = i + 1;
		    			break;
		    		}
		    	}
		    	return search(node.getChild(index,depth+1==height),key,depth+1);
		    }
		}
	}
	
	/**
	 * This method is used to search the first index that has the key array 
	 * in the parameter as the prefix. 
	 * @param Node the node that will be searched.
	 * @param key the key that will be used for searching.
	 * @return the index which is the smallest valid one, -1 means there is 
	 * no tuple available in the list.
	 */
	private int searchIndex(Node temp, DataType[] key) {
		int index = 0;
		List<DataType[]> list = temp.keylist;
		for(;index<list.size();index++) {
			if(compare.compareArray(key,list.get(index))==0)
				break;
		}
		return index == list.size() ? -1 : index;
	}
	
	/**
	 * This method is used to search the ending point of the valid key.
	 * @param dummy the node that is used for searching.
	 * @param index the starting point of the search.
	 * @param key the key used for checking.
	 * @param result the array list that stores the table entries.
	 * @return the index that is the ending point.
	 */
	private int searchEnd(Node dummy, int index, DataType[] key,
							List<int[]> result) {
		List<DataType[]> list = dummy.keylist;
		List<List<int[]>> entrylist = ((LeafNode)dummy).valuelist;
		while(index < list.size()) {
			DataType[] temp = list.get(index);
			if(compare.compareArray(key,temp)!=0)
				break;
			for(int[] pair : entrylist.get(index))
				result.add(pair);
			index++;
		}
		return index;
	}
	
	/**
	 * This method is used to get the next Node of the current node which 
	 * has the file location as the argument.
	 * @param filelocation the file location of the current node.
	 * @param height the current height of the node.
	 * @return the Node which is the next node of the current node.
	 * Null means there are no nodes available for the current one.
	 */
	private Node getNextNode(String filelocation, int height) {
		/* this indicates we are at the upper bound of the root node, so
		 * technically there are no nodes left, so simply return null. */
		if(height==0) return null;
		int index = filelocation.length() - 1;
		String numberindex = "";
		while(filelocation.charAt(index)!='/') {
			numberindex = filelocation.charAt(index) + numberindex;
			index--;
		}
		int nextindex = Integer.parseInt(numberindex) + 1;
		String residue = filelocation.substring(0,index);
		String newfilelocation = residue + "/" + nextindex;
		File checkfile = new File(newfilelocation+"/");
		/* this indicates we have reach the end of the current node, 
		 * move to the parent node and do the same thing recursively. */
		if(!checkfile.exists()) 
			return getNextNode(residue, height - 1);
		else {
			/* note if the height is not the same with the height of the 
			 * B+ Tree, it means we do not reach a leaf node. iteratively
			 * add "/0" until we reach the leaf node. */
			while(this.height != height) {
				newfilelocation += "/0";
				height++;
			}
			return new LeafNode(newfilelocation);
		}
	}
	
	/**
	 * This method is used to insert a key/value pair into the BPlusTree
	 * Note if there is an overflow coming from a node, we need to split
	 * the node into two parts and push a key value up, do that recursively
	 * if the index node is also overflowed.
	 * @param key the key that will be inserted.
	 * @param value an array of two integers that indicates the location of 
	 * a tuple.
	 */
	public void insert(DataType[] key, int[] value) {
        Node temp = root;
        insert(temp,key,value, 0);
	}

	/**
	 * This helper method is used to insert a key in the tree.
	 * @param temp the current node which is the target of modification.
	 * @param key the key that will be inserted.
	 * @param value an array of two integers that indicates the location of 
	 * a tuple.
	 * @param the depth of the current node.
	 * @return the data type that needs to be inserted.
	 */
	private Entry<DataType[], Node> insert(Node temp, DataType[] key,
											int[] value, int depth) {
		/* this usually indicates the root is empty, we should initialize
		 * a leaf node and assign it to the root. */
		if(temp==null){
			initialize(key,value);
			root = new LeafNode(location);
			return null;
		}
		else if(depth == height){
			LeafNode node = (LeafNode)temp;
			node.insertSorted(key, value);
			if(!node.isOverflowed()) {
				node.writeFile();
				return null;
			}
			else{
				Entry<DataType[], Node> enter = splitLeafNode(node);
				if(temp!=root) return enter;
				/* the root is overflowed, need to create a new root. */
				else{
					Node leftChild = new LeafNode(location + "/0",
												node.keylist, node.valuelist);
					List<DataType[]> keylist = new ArrayList<>();
					keylist.add(enter.getKey());
					root = new IndexNode(location, keylist);
					height++;
					return null;
				}
			}
		}
		else{
			IndexNode node = (IndexNode)temp;
			Node inserted = null;
			int index = 0;
			/* check the desired node to dive into. */
			if(compare.compareArray(key, (temp.keylist.get(0)))<0)
				inserted = node.getChild(0, depth + 1 == height);
			else if(compare.compareArray(key, 
					temp.keylist.get(temp.keylist.size()-1))>=0){
				index = temp.keylist.size();
				inserted = node.getChild(index, depth + 1 == height);
			}else{
				for(int i=0;i<temp.keylist.size()-1;i++){
					if(compare.compareArray(key, temp.keylist.get(i))>=0
							&&compare.compareArray(key, temp.keylist.get(i+1))<0){
						index = i + 1;
						break;
					}
				}
				inserted = node.getChild(index, depth + 1 == height);
			}
			// insert the node recursively.
			Entry<DataType[], Node> enter = insert(inserted,key,value,depth+1);
			if(enter==null) return null;
			else{
				node.insertSorted(enter.getKey(),index);
				// if the node is not overflowed anymore, set the return value to null.
				if(!node.isOverflowed()) {
					node.writeFile();
					return null;
				}
				else{
					enter = splitIndexNode(node);
					if(temp!=root) return enter;
					else{
						Node<K,T> leftChild = node;
						Node<K,T> rightChild = enter.getValue();
						root = new IndexNode<K,T>(enter.getKey(),leftChild,rightChild);
						return null;
					}
				}
			}
		}
	}
	
	/**
	 * This method is used to initialize the key file for the root node.
	 * Note the file should follow the format of the leaf node.
	 * @param key the key that will be used to write a file.
	 * @param value the pair of integers indicates the index of a tuple.
	 */
	private void initialize(DataType[] key, int[] value) {
		File newkey = new File(location + "/keylist");
		try {
			FileOutputStream fout = new FileOutputStream(newkey);
			FileChannel fc = fout.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
			buffer.putInt(0, key.length);
			int start = 4;
			for(int i=0;i<key.length;i++) {
				buffer.putInt(start, key[i].getType());
				start += 4;
			}
			buffer.putInt(start, 1);
			start = writeKey(buffer, start + 4, key);
			buffer.putInt(start, value[0]);
			buffer.putInt(start + 4, value[1]);
			buffer.limit(buffer.capacity());
			buffer.position(0);
			fc.write(buffer);
			fout.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This method is used to write the key elements back to the byte buffer.
	 * @param buffer the byte buffer that we put data in.
	 * @param start the starting point of the buffer page.
	 * @param key an array of key that will be inserted.
	 * @return the ending point of the key in the buffer page.
	 */
	private int writeKey(ByteBuffer buffer, int start, DataType[] key) {
		for(int i=0;i<key.length;i++) {
			DataType data = key[i];
			int increment = 8;
			if(key[i].getType()==1)
				buffer.putLong(start, data.getLong());
			else if(key[i].getType()==5)
				buffer.putDouble(start, data.getDouble());
			else if(key[i].getType()==2) {
				String str = data.getString();
				buffer.put(start, (byte)str.length());
				start++;
				for(char c : str.toCharArray()) {
					buffer.put(start, (byte)c);
					start++;
				}
				increment = 0;
			}
			start += increment;
		}
		return start;
	}
	
	/**
	 * Split a leaf node and return the new right node and the splitting
	 * key as an Entry<slitingKey, RightNode>
	 * @param leaf, any other relevant data
	 * @return the key/node pair as an Entry
	 */
	public Entry<DataType[], Node> splitLeafNode(LeafNode leaf) {
		DataType[] key = leaf.keylist.get(KEY_LIMITS);
        List<DataType[]> list1 = new ArrayList<>();
        List<List<int[]>> list2 = new ArrayList<>();
        for(int i=KEY_LIMITS;i<leaf.keylist.size();){
        	list1.add(leaf.keylist.remove(KEY_LIMITS));
        	list2.add(leaf.valuelist.remove(KEY_LIMITS));
        }
        /* we need to rewrite the file that is splited. */
        leaf.writeFile();
        Node result = moveLeafFiles(leaf.filelocation,list1,list2);
        return new SimpleEntry(key, result);
	}
	
	/**
	 * This method is used to move the files by having their indexes incremented.
	 * @param location the location of the staring point.
	 * @param list1 the key list that needs to be initialized.
	 * @param list2 the value list that needs to be initialized.
	 * @param return the right one of the modified node.
	 */
	private Node moveLeafFiles(String location, List<DataType[]> list1,
							List<List<int[]>> list2) {
		/* notice we need to take care of the case when the root is the 
		 * node that will be split. */
		if(location.equals(this.location)) {
			File leftchilddir = new File(location + "/0/");
			if(!leftchilddir.mkdir())
				System.out.println("Faild to create directories!");
			File rightchilddir = new File(location + "/1/");
			if(!rightchilddir.mkdir())
				System.out.println("Faild to create directories!");
			Node split = new LeafNode(location + "/1", list1, list2);
			return split;
		}
		return moveFiles(list1,list2,true);
	}

	/**
	 * TODO split an indexNode and return the new right node and the splitting
	 * key as an Entry<slitingKey, RightNode>
	 * 
	 * @param index, any other relevant data
	 * @return new key/node pair as an Entry
	 */
	public Entry<DataType[], Node> splitIndexNode(IndexNode index) {
        DataType[] key = index.keylist.remove(KEY_LIMITS);
        List<DataType[]> list1 = new ArrayList<>();
        // get the right most one of the new entry out first.
        for(int i=KEY_LIMITS;i<index.keylist.size();){
        	list1.add(index.keylist.remove(KEY_LIMITS));
        }
        Node node = moveIndexFiles(index.filelocation, list1);
        Entry<K, Node<K,T>> entry = new SimpleEntry<>(key,node);
		return entry;
	}
	
	/**
	 * This method is similar to moving leaf files: by having their indices
	 * incremented.
	 * @param filelocation the location of the file.
	 * @param keylist the list that stores the keys.
	 * @return the right one of the modified node.
	 */
	private Node moveIndexFiles(String location, List<DataType[]> keylist) {
		/* As usual, we need to check if the node we manipulate is 
		 * the root node. */
		if(this.location.equals(location)) {
			
		}
		
		
	}
	
	private Node moveFiles(List<DataType[]> list1, List<List<int[]>> list2,
							boolean isLeaf) {
		int index = location.length() - 1;
		String current = "";
		while(location.charAt(index)!='/') {
			current = location.charAt(index) + current;
			index--;
		}
		int endMoving = Integer.parseInt(current) + 1;
		String parent = location.substring(0, index);
		Node parentnode = new IndexNode(parent);
		int limit = parentnode.keylist.size() + 1;
		for(int i=limit;i>endMoving;i--) {
			String deport = parent + "/" + (i - 1) + "/";
			String destination = parent + "/" + i + "/";
			moveDir(deport, destination);
		}
		/* do not forget to assign the right part of the splitted node! */
		Node split = new LeafNode(parent + "/" + endMoving, list1, list2);
		return split;
	}
	
	/**
	 * This method is used to move the whole file directory from one location
	 * to another. Basically we do this recursively to move all the files.
	 * @param out the old location of the file.
	 * @param in the new location of the file.
	 */
	private void moveDir(String out, String in) {
		File filedir = new File(out);
		File existdir = new File(in);
		if(!existdir.exists())
			if(!existdir.mkdirs())
				System.out.println("Failed to create the directory!");
		/* recall the format of the B+ tree: for a single node, only one file
		 * that contains the key list and a list of directories starting from 0.
		 * so we could move the single file first and move on to the directory 
		 * by doing that recursively. */
		int index = 0;
		while(true) {
			File keyfile = new File(out + "keylist");
			if(!keyfile.renameTo(new File(in + "keylist")))
				System.out.println("Failed to move the file!");
			String newout = out + index + "/";
			String newin = in + index + "/";
			File indexdir = new File(newout);
			if(indexdir.exists())
				moveDir(newout, newin);
			else break;
			index++;
		}
	}

	/**
	 * TODO Delete a key/value pair from this B+Tree
	 * 
	 * @param key
	 */
	public void delete(K key) {
        Node<K,T> temp = root;
        delete(temp,null,null,null,key);
	}
	
	private int delete(Node<K,T> temp, Node<K,T> left, Node<K,T> right, IndexNode<K,T> parent, K key) {
		if(temp==null) return -1;
		else if(temp.isLeafNode){
			LeafNode<K,T> node = (LeafNode<K,T>)temp;
			int index = temp.keys.indexOf(key);
			if(index==-1) return -1; // handle the case when the key is not exist.
			else{
				temp.keys.remove(index);
				node.values.remove(index);
				if(!temp.isUnderflowed()) return -1;
				else if(parent==null){
					// we are at the root now and no keys left, set root to null.
				    if(temp.keys.size()==0) root = null;
				    return -1;
				}else{
					LeafNode<K,T> leftnode = (LeafNode<K,T>)left;
					LeafNode<K,T> rightnode = (LeafNode<K,T>)right;
					return handleLeafNodeUnderflow(leftnode,rightnode,parent);
				}
			}
		}else{
			IndexNode<K,T> node = (IndexNode<K,T>)temp;
			Node<K,T> target = null;
			Node<K,T> leftnode = null;
			Node<K,T> rightnode = null;
			int index = 0;
			// get the desired nodes from the list.
			if(key.compareTo(temp.keys.get(0))<0){
				target = node.children.get(0);
				rightnode = node.children.get(1);
			}else if(key.compareTo(temp.keys.get(temp.keys.size()-1))>=0){
				target = node.children.get(node.children.size()-1);
				leftnode = node.children.get(node.children.size()-2);
			}else{
				for(int i=0;i<temp.keys.size()-1;i++){
					if(key.compareTo(temp.keys.get(i))>=0&&key.compareTo(temp.keys.get(i+1))<0){
						index = i + 1;
						break;
					}
				}
				target = node.children.get(index);
				leftnode = node.children.get(index-1);
			}
			// delete the node recursively.
			int delete = delete(target,leftnode,rightnode,node,key);
			if(delete==-1) return -1;
			else{
				// there is a merge, need to delete a key.
				node.keys.remove(delete);
				node.children.remove(delete+1);
				if(!node.isUnderflowed()) return -1;
				else if(parent==null){
					// we reach the root and there are no keys left. Reset the root.
					if(node.keys.size()==0) root = node.children.get(0);
					return -1;
				}
				else{
					IndexNode<K,T> leftnood = (IndexNode<K,T>)left;
					IndexNode<K,T> rightnood = (IndexNode<K,T>)right;
					return handleIndexNodeUnderflow(leftnood,rightnood,parent);
				}
			}
		}
	}

	/**
	 * TODO Handle LeafNode Underflow (merge or redistribution)
	 * 
	 * @param left
	 *            : the smaller node
	 * @param right
	 *            : the bigger node
	 * @param parent
	 *            : their parent index node
	 * @return the splitkey position in parent if merged so that parent can
	 *         delete the splitkey later on. -1 otherwise
	 */
	public int handleLeafNodeUnderflow(LeafNode<K,T> left, LeafNode<K,T> right,
			IndexNode<K,T> parent) {
        int dummy = 0;
		if(left!=null&&left.keys.size()>D){
			dummy = parent.children.indexOf(left) + 1;
			LeafNode<K,T> mid = (LeafNode<K,T>)parent.children.get(dummy);
			int temp = (left.keys.size()+D-1)-(left.keys.size()+D)/2;
			// evenly handle the nodes.
			for(int i = left.keys.size()-1;i>=temp;i--){
				mid.keys.add(0,left.keys.remove(i));
				mid.values.add(0,left.values.remove(i));
			}
			// set the new key value for the parent node.
			parent.keys.set(dummy-1, mid.keys.get(0));
			return -1;
		}
		else if(left!=null){
			dummy = parent.children.indexOf(left) + 1;
			LeafNode<K,T> mid = (LeafNode<K,T>)parent.children.get(dummy);
			// merge the two nodes.
			for(int i=0;i<mid.keys.size();){
				left.keys.add(mid.keys.remove(0));
				left.values.add(mid.values.remove(0));
			}
			return dummy - 1; // return the index that is gonna be deleted.
		}
		if(right!=null&&right.keys.size()>D){
			dummy = parent.children.indexOf(right) - 1;
			LeafNode<K,T> mid = (LeafNode<K,T>)parent.children.get(dummy);
			int temp = (right.keys.size()+D-1)-(right.keys.size()+D)/2;
			// evenly handle the nodes.
			for(int i = mid.keys.size();i<temp;i++){
				mid.keys.add(right.keys.remove(0));
				mid.values.add(right.values.remove(0));
			}
			// set the new key value for the parent node.
			parent.keys.set(dummy, right.keys.get(0));
			return -1;
		}		
		else if(right!=null){
			dummy = parent.children.indexOf(right) - 1;
			LeafNode<K,T> mid = (LeafNode<K,T>)parent.children.get(dummy);
			// merge the two nodes.
			for(int i=0;i<right.keys.size();){
				mid.keys.add(right.keys.remove(0));
				mid.values.add(right.values.remove(0));
			}
			return dummy; // return the index that is gonna be deleted.
		}
		return -1;

	}

	/**
	 * TODO Handle IndexNode Underflow (merge or redistribution)
	 * 
	 * @param left
	 *            : the smaller node
	 * @param right
	 *            : the bigger node
	 * @param parent
	 *            : their parent index node
	 * @return the splitkey position in parent if merged so that parent can
	 *         delete the splitkey later on. -1 otherwise
	 */
	public int handleIndexNodeUnderflow(IndexNode<K,T> leftIndex,
			IndexNode<K,T> rightIndex, IndexNode<K,T> parent) {
		int dummy = 0;
		if(leftIndex!=null&&leftIndex.keys.size()>D){
			dummy = parent.children.indexOf(leftIndex) + 1;
			IndexNode<K,T> mid = (IndexNode<K,T>)parent.children.get(dummy);
			mid.keys.add(0,parent.keys.get(dummy-1));
			int temp = (leftIndex.keys.size()+D-1)-(leftIndex.keys.size()+D)/2;
			// evenly handle the nodes.
			for(int i = leftIndex.keys.size()-1;i>=temp;i--){
				mid.keys.add(0,leftIndex.keys.remove(i));
				mid.children.add(0,leftIndex.children.remove(i+1));
				// pay attention on the difference of the index!
			}
			// set the new key value for the parent node.
			parent.keys.set(dummy-1, mid.keys.remove(0));
			return -1;
		}
		else if(leftIndex!=null){
			dummy = parent.children.indexOf(leftIndex) + 1;
			IndexNode<K,T> mid = (IndexNode<K,T>)parent.children.get(dummy);
			leftIndex.keys.add(parent.keys.get(dummy-1));
			leftIndex.children.add(mid.children.remove(0));
			// merge the two nodes.
			for(int i=0;i<mid.keys.size();){
				leftIndex.keys.add(mid.keys.remove(0));
				leftIndex.children.add(mid.children.remove(0));
			}
			return dummy - 1; // return the index that is gonna be deleted.
		}
		if(rightIndex!=null&&rightIndex.keys.size()>D){
			dummy = parent.children.indexOf(rightIndex) - 1;
			IndexNode<K,T> mid = (IndexNode<K,T>)parent.children.get(dummy);
			mid.keys.add(parent.keys.get(dummy));
			int temp = (rightIndex.keys.size()+D-1)-(rightIndex.keys.size()+D)/2;
			// evenly handle the nodes.
			for(int i = mid.keys.size();i<=temp;i++){
				mid.keys.add(rightIndex.keys.remove(0));
				mid.children.add(rightIndex.children.remove(0));
			}
			// set the new key value for the parent node.
			parent.keys.set(dummy, mid.keys.remove(mid.keys.size()-1));
			return -1;
		}		
		else if(rightIndex!=null){
			dummy = parent.children.indexOf(rightIndex) - 1;
			IndexNode<K,T> mid = (IndexNode<K,T>)parent.children.get(dummy);
			mid.keys.add(parent.keys.get(dummy));
			mid.children.add(rightIndex.children.remove(0));
			// merge the two nodes.
			for(int i=0;i<rightIndex.keys.size();){
				mid.keys.add(rightIndex.keys.remove(0));
				mid.children.add(rightIndex.children.remove(0));
			}
			return dummy; // return the index that is gonna be deleted.
		}
		return -1;
	}
	
}
