package ch.epfl.dias.ops.block;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements BlockOperator {

	// TODO: Add required structures
	private BlockOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(BlockOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
        this.child = child;
        this.op = op;
        this.fieldNo = fieldNo;
        this.value = value;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement
        DBColumn[] columns = child.execute();
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

        DBColumn[] result = new DBColumn[columns.length];

        for (int i = 0; i < tempResult.size(); i++) {
            result[i] = new DBColumn(tempResult.get(i).toArray(), columns[i].type);
        }

		return result;
	}
}
