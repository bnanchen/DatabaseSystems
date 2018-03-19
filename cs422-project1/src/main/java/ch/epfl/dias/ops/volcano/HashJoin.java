package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {

	// TODO: Add required structures
	private VolcanoOperator leftChild;
	private VolcanoOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private HashMap<Object, ArrayList<DBTuple>> hashes = new HashMap<>();
	private DBTuple rightNext;
	private int iter = 0;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.leftFieldNo = leftFieldNo;
        this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
        leftChild.open();
        rightChild.open();
        DBTuple next = leftChild.next();
        while(!next.eof) {
            if (hashes.get(next.fields[leftFieldNo]) != null) {
                hashes.get(next.fields[leftFieldNo]).add(next);
            } else {
                ArrayList<DBTuple> listTuples = new ArrayList<>();

                listTuples.add(next);
                hashes.put(next.fields[leftFieldNo], listTuples);
            }
            next = leftChild.next();
        }
        rightNext = rightChild.next();
	}

	@Override
	public DBTuple next() {
		// TODO: Implement
        while (!rightNext.eof) {
            if (hashes.get(rightNext.fields[rightFieldNo]) != null && hashes.get(rightNext.fields[rightFieldNo]).size() > iter) {
                DBTuple returnedTuple = reconstructTuple(hashes.get(rightNext.fields[rightFieldNo]).get(iter), rightNext);
                iter++;
                return returnedTuple;
            }
            rightNext = rightChild.next();
            iter = 0;
        }
		return rightNext;
	}

	private DBTuple reconstructTuple(DBTuple leftTuple, DBTuple rightTuple) {
        Object[] resultTuple = new Object[leftTuple.fields.length+rightTuple.fields.length-1];
        DataType[] resultDT = new DataType[leftTuple.types.length+rightTuple.types.length-1];
        System.arraycopy(leftTuple.fields, 0, resultTuple, 0, leftTuple.fields.length);
        System.arraycopy(leftTuple.types, 0, resultDT, 0, leftTuple.types.length);
        if (leftFieldNo == 0) {
            System.arraycopy(rightTuple.fields, 1, resultTuple, leftTuple.fields.length, rightTuple.fields.length-1);
            System.arraycopy(rightTuple.types, 1, resultDT, leftTuple.types.length, rightTuple.types.length-1);
        } else {
            System.arraycopy(rightTuple.fields, 0, resultTuple, leftTuple.fields.length, rightFieldNo+1);
            System.arraycopy(rightTuple.fields, rightFieldNo+1, resultTuple, leftTuple.fields.length+rightFieldNo, rightTuple.fields.length-(rightFieldNo+1));
            System.arraycopy(rightTuple.types, 0, resultTuple, leftTuple.types.length, rightFieldNo+1);
            System.arraycopy(rightTuple.types, rightFieldNo+1, resultTuple, leftTuple.types.length+rightFieldNo, rightTuple.types.length-(rightFieldNo+1));
        }
	    return new DBTuple(resultTuple, resultDT);
    }

	@Override
	public void close() {
		// TODO: Implement
        leftChild.close();
        rightChild.close();
	}
}
