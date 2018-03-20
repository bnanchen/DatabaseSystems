package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;

public class Select implements VectorOperator {

	// TODO: Add required structures
	private VectorOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
        this.child = child;
        this.op = op;
        this.fieldNo = fieldNo;
        this.value = value;
	}
	
	@Override
	public void open() {
		// TODO: Implement
        child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
        DBColumn[] columns = child.next();
        DataType[] dtArray = new DataType[columns.length];
        for (int i = 0; i < columns.length; i++) {
            dtArray[i] = columns[i].type;
        }
        if (columns[0].eof) {
            DBColumn[] empty = {new DBColumn()};
            return empty;
        }

        ArrayList<ArrayList<Object>> resultList = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            ArrayList<Object> temp = new ArrayList<>();
            resultList.add(temp);
        }

        while (!columns[0].eof) {
            ArrayList<ArrayList<Object>> tempResult = new ArrayList<>();
            for (int i = 0; i < columns.length; i++) {
                ArrayList<Object> temp = new ArrayList<>();
                temp.addAll(Arrays.asList(columns[i].column));
                tempResult.add(temp);
            }
            Integer[] column = columns[fieldNo].getAsInteger();
            boolean testResult;
            int index = 0;
            for (int compareTo : column) {
               // System.out.println(compareTo);
                switch (op) {
                    case GT: testResult = compareTo > value;
                        break;
                    case NE: testResult = compareTo != value;
                        break;
                    case EQ: testResult = compareTo == value;
                        break;
                    case GE: testResult = compareTo >= value;
                        break;
                    case LE: testResult = compareTo <= value;
                        break;
                    case LT: testResult = compareTo < value;
                        break;
                    default: throw new IllegalArgumentException("Not a correct test operand.");
                }
                if (!testResult) {
                    for (int i = 0; i < columns.length; i++) {
                        tempResult.get(i).remove(index);
                    }
                } else {
                    index++;
                }
            }
            for (int i = 0; i < resultList.size(); i++) {
                for (int j = 0; j < tempResult.get(0).size(); j++) {
                    resultList.get(i).add(tempResult.get(i).get(j));
                }
                //resultList.get(i).add(tempResult.get(i));
            }
            columns = child.next();
        }

        DBColumn[] result = new DBColumn[resultList.size()];

        for (int i = 0; i < resultList.size(); i++) {
           // System.out.println("size "+ resultList.get(i).size());
            result[i] = new DBColumn(resultList.get(i).toArray(), dtArray[i]);
        }
//        System.out.println("------");
//        for (int i = 0; i < result[0].column.length; i++) {
//            System.out.println(result[0].column[i]);
//        }
        return result;
	}

	@Override
	public void close() {
		// TODO: Implement
        child.close();
	}
}
