package Sorting;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import Evaluator.Evaluator;
import PhysicalOperators.Operator;
import SQLExpression.Expression;
import SmallSQLServer.Main;
import Support.Mule;
import TableElement.DataType;
import TableElement.Tuple;

/** 
 * This class mainly holds the external sort operation.
 * The logic of the sort will be displayed in the methods below.
 * @author messfish
 *
 */
public class ExternalSort {

	private static final int NUM_OF_BUFFER = 10;
	private static final int NUM_OF_BYTES = 16384;
	private int file_index = 1;
	private Map<String, Mule> schema;
	private List<Expression> attributeslist;
	private File result; // this will be used to store the result.
	private Operator op;
	private Tuple first; // this is used to store the previous tuple 
	// that could not be stored in the buffer page.
	
	/**
	 * Constructor: this constructor is used to fetch all the tuples
	 * in an operator and store them in pages. After that, it would 
	 * perform these two types of sorting: (1) First it stores the 
	 * pages in the buffer poll and sort them and put them in one 
	 * file. (2) Next, use all but one pages in the buffer pool to 
	 * store the files. Pick the smallest of them and write that into
	 * the empty slot. Note we need to do everything in a single page.
	 * Do this iteratively until only one large file left. Pass that 
	 * file to the global file operator.
	 * @param op the operator that calls this class.
	 * @param attributeslist the list of attributes that used for sorting.
	 * @param ID this marks the ID of the operator who are calling this class.
	 */
	public ExternalSort(Operator op, List<Expression> attributeslist, int ID) {
		this.op = op;
		schema = op.getSchema();
		this.attributeslist = attributeslist;
		/* At first, we build the base of the sorting file. */
		File file = null, previous = null;
		while(true) {
			previous = file;
			file = writeBase(op, ID);
			if(file == null)
				break;
			file_index++;
		}
		/* if the file index is 1, that means there are no tuples available.
		 * do nothing and let the result be a null pointer. */
		/* this indicates all the tuples are sorted during the first step,
		 * so we simply assign the file to the result file. */
		if(file_index == 2) {
			result = previous;
		}else if(file_index > 2) {
			merge(ID);
			file_index--;
			result = new File(Main.getTemp() + "/" + ID + " " + file_index);
		}
	}
	
	/**
	 * This is the getter method of the schema.
	 * @return the schema of the external sort class.
	 */
	public Map<String, Mule> getSchema() {
		return schema;
	}
	
	/**
	 * This is the getter method of the result file.
	 * @return the result file.
	 */
	public File getResult() {
		return result;
	}
	
	/**
	 * This method is used to write the base of the file. Get all the tuples
	 * available from the operator and store them in the file.
	 * @param op the operator needed for sorting.
	 * @param ID the ID to identify the table.
	 * @return the file that contains sorted data.
	 */
	private File writeBase(Operator op, int ID) {
		List<Tuple> list = new ArrayList<>();
		Tuple tuple = null;
		for(int i=0;i<NUM_OF_BUFFER;i++) {
			tuple = storePage(op, tuple, list);
			if(tuple == null)
				break;
		}
		/* this indicates no more tuples left, simply return null. */
		if(list.size()==0) return null;
		else if(tuple != null)
			list.add(tuple);
		Collections.sort(list, new Comparator<Tuple>(){
			@Override
			public int compare(Tuple t1, Tuple t2) {
				return comparison(t1, t2);
			}
		});
		File file = new File(Main.getTemp() + "/" + ID + " " + file_index);
		try{
			FileOutputStream output = new FileOutputStream(file);
			FileChannel fc = output.getChannel();
			ByteBuffer buffer = null;
			int start = 0;
			while((buffer=writePage(list, start))!=null) {
				start += buffer.getInt(0);
				buffer.limit(buffer.capacity());
				buffer.position(0);
				fc.write(buffer);
			}
			output.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
		return file;
	}
	
	/**
	 * This method is used for storing the tuple into a tuple list. Notice the 
	 * amount of increment should not exceed the size of a single page. Also,
	 * since there are no needs to get the tuple ID and the byte to indicate
	 * the state of the tuple. We could simply leave them out.
	 * @param op the operator to extract the tuple.
	 * @param tuple might stores the tuple that 
	 * @param list the list that stores the tuples.
	 * @return tuple the next tuple that cannot fit into the page.
	 */
	private Tuple storePage(Operator op, Tuple tuple, List<Tuple> list) {
		int index = 4;
		if(tuple!=null) {
			list.add(tuple);
			index += op.checkSize(tuple);
		}
		while((tuple=op.getNextTuple())!=null) {
			index += op.checkSize(tuple);
			if(index > NUM_OF_BYTES)
				return tuple;
			list.add(tuple);
		}
		/* when we meet this code, that means there are no tuples left
		 * in the operator, so we simply return null. */ 
		return null;
	}
	
	/**
	 * This method is mainly used for comparing two different tuples
	 * by using the schema and the attribute list. Note if we cannot
	 * tell apart from the attributes list, we use the rest of the 
	 * attributes to pull them apart. Return 0 if we find these two
	 * tuples are actually equal.
	 * @param t1 one of the tuples to be compared.
	 * @param t2 one of the tuples to be compared.
	 * @return an integer to show which is bigger, 1 means t1 is bigger
	 * than t2, -1 means t1 is smaller than t2. 0 means they are equally
	 * the same.
	 */
	int comparison(Tuple t1, Tuple t2) {
		for(int i=0;i<attributeslist.size();i++) {
			Expression exp = attributeslist.get(i);
			Evaluator eva1 = new Evaluator(t1, exp, schema);
			DataType data1 = eva1.getData();
			Evaluator eva2 = new Evaluator(t2, exp, schema);
			DataType data2 = eva2.getData();
			if(data1.compare(data2)!=0)
				return data1.compare(data2);
		}
		for(int i=0;i<t1.datasize();i++){
			DataType data1 = t1.getData(i);
			DataType data2 = t2.getData(i);
			if(data1.compare(data2)!=0)
				return data1.compare(data2);
		}
		return 0;
	}
	
	/**
	 * This method is used for writing the tuples as bytes in the byte
	 * buffer. We start the tuple from the starting point and moves on
	 * until there is not enough room for the tuple at the current index.
	 * @param list the list of Tuples 
	 * @param start the starting point of the list
	 * @return a byte buffer that stores the data.
	 */
	private ByteBuffer writePage(List<Tuple> list, int start) {
		if(list.size()==start)
			return null;
		ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
		int temp = start, index = 4;
		while(start<list.size()) {
			Tuple tuple = list.get(start);
			int length = op.checkSize(tuple);
			if(index + length > NUM_OF_BYTES) 
				break;
			op.writeTuple(buffer, tuple, index);
			index += length;
			start++;
		}
		buffer.putInt(0, start - temp);
		return buffer;
	}
	
	/**
	 * This is the second part of the external sort: bring the files
	 * to fill up all but one slots in the buffer page. Pick the 
	 * smallest tuple and store that into the empty slot. In other words,
	 * merge the file array into one file.
	 * @param ID the ID that used to identify the operator which calls this class.
	 */
	private void merge(int ID) {
		int current = 1;
		/* only when we get all the data in one file can we break out
		 * from the condition. */
		while(file_index - current > 1) {
			int limit = file_index;
			while(current < limit) {
				TempOperator[] temparray = new TempOperator[NUM_OF_BUFFER-1];
				int numofoperators = 0;
				for(;numofoperators<NUM_OF_BUFFER - 1&&current<limit;
						numofoperators++,current++) {
					String filelocation = Main.getTemp() + "/" + ID + " " + current;
					File file = new File(filelocation);
					temparray[numofoperators] = 
							new TempOperator(file, op.getSchema());
				}
				PriorityQueue<HeapData> pq = new PriorityQueue<>
						((a,b)->comparison(a.getTuple(),b.getTuple()));
				for(int i=0;i<numofoperators;i++) 
					pq.offer(new HeapData(i, temparray[i].getNextTuple()));
				writeFile(pq,temparray,ID);
				file_index++;
			}
		}
	}
	
	/**
	 * This method is mainly used for writing the data into the file.
	 * @param pq the priority queue to fetch the minimum data out.
	 * @param temparray the array of temp operators.
	 * @param ID the ID of the operators using this class.
	 */
	private void writeFile(PriorityQueue<HeapData> pq, 
						   TempOperator[] temparray, int ID) {
		File file = new File(Main.getTemp() + "/" + ID + " " + file_index);
		try {
			FileOutputStream out = new FileOutputStream(file);
			FileChannel fc = out.getChannel();
			while(!pq.isEmpty()) {
				ByteBuffer buffer = writePage(pq, temparray);
				buffer.limit(buffer.capacity());
				buffer.position(0);
				fc.write(buffer);
			}
			first = null;
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This method is used to extract the tuple from the priority 
	 * queue and store the result into the byte buffer.
	 * @param pq the priority queue used to extract the smallest tuple out.
	 * @param temparray the array of operators that stores tuple.
	 * @return the byte buffer that stores the data.
	 */
	private ByteBuffer writePage(PriorityQueue<HeapData> pq,
						TempOperator[] temparray) {
		ByteBuffer buffer = ByteBuffer.allocate(NUM_OF_BYTES);
		int index = 4, times = 0;
		if(first!=null) {
			op.writeTuple(buffer, first, index);
			index += op.checkSize(first);
			times++;
		}
		while(!pq.isEmpty()) {
			HeapData mule = pq.poll();
			Tuple tuple = mule.getTuple();
			int length = op.checkSize(tuple);
			int arrayindex = mule.getIndex();
			Tuple next = temparray[arrayindex].getNextTuple();
			if(next != null)
				pq.offer(new HeapData(arrayindex, next));
			else temparray[arrayindex].close();
			if(index + length > NUM_OF_BYTES) {
				first = tuple;
				break;
			}
			op.writeTuple(buffer,tuple,index);
			index += length;
			times++;
		}
		buffer.putInt(0, times);
		return buffer;
	}
	
}
