package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class ColumnarTest {

	DataType[] orderSchema;
	DataType[] lineitemSchema;
	DataType[] schema;

	ColumnStore columnstoreData;
	ColumnStore columnstoreOrder;
	ColumnStore columnstoreLineItem;
	ColumnStore columnstoreOrderBig;
	ColumnStore columnstoreLineItemBig;

	@Before
	public void init() {

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		columnstoreData.load();

		columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
		columnstoreOrder.load();

		columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
		columnstoreLineItem.load();
		
		columnstoreOrderBig = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
		columnstoreOrderBig.load();
		
		columnstoreLineItemBig = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
		columnstoreLineItemBig.load();
	}

	@Test
	public void spTestData() {
		/* SELECT COUNT(*) FROM data WHERE col4 == 6 */
		ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreData);
		ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void spTestOrder() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 6 */
		ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreOrder);
		ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 0, 6);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 1);
	}

	@Test
	public void spTestLineItem() {
		/* SELECT COUNT(*) FROM data WHERE col0 == 3 */
		ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
		ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT,
				DataType.INT, 2);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

    @Test
    public void testOrderSumInt() {
	    // with int
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, 0);

        DBColumn[] result = agg.execute();

        int output = result[0].getAsInteger()[0];
        assertTrue(output == 17);
    }

    @Test
    public void testOrderSumDouble() {
        // with int
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(scan, Aggregate.SUM, DataType.DOUBLE, 5);

        DBColumn[] result = agg.execute();

        double output = result[0].getAsDouble()[0];
        assertTrue(output == 454890.80);
    }

    @Test
    public void testOrderAvg() {
        // with double
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 5);

        // with int
        ch.epfl.dias.ops.block.Scan scan_ = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.block.ProjectAggregate agg_ = new ch.epfl.dias.ops.block.ProjectAggregate(scan_, Aggregate.AVG, DataType.INT, 0);

        DBColumn[] result = agg.execute();
        DBColumn[] result_ = agg_.execute();
        double output = result[0].getAsDouble()[0];
        double output_ = result_[0].getAsInteger()[0];
        assertTrue(output == 45489.08);
        assertTrue(output_ == 1);
    }

    @Test
    public void testOrderMaxDouble() {
        // with double
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(scan, Aggregate.MAX, DataType.DOUBLE, 6);

        DBColumn[] resultDouble = agg.execute();
        double outputDouble = resultDouble[0].getAsDouble()[0];
        assertTrue(outputDouble == 0.10);
    }

    @Test
    public void testOrderMaxInt() {
        // with int
        ch.epfl.dias.ops.block.Scan scanInt = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.block.ProjectAggregate aggInt = new ch.epfl.dias.ops.block.ProjectAggregate(scanInt, Aggregate.MAX, DataType.INT, 0);

        DBColumn[] resultInt = aggInt.execute();
        int outputInt = resultInt[0].getAsInteger()[0];
        assertTrue(outputInt == 3);
    }

    @Test
    public void testOrderMaxString() {
        // with string
        ch.epfl.dias.ops.block.Scan scanString = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.block.ProjectAggregate aggString = new ch.epfl.dias.ops.block.ProjectAggregate(scanString, Aggregate.MAX, DataType.STRING, 15);

        DBColumn[] resultString = aggString.execute();
        String outputString = resultString[0].getAsString()[0];
        assertTrue(outputString.equals("ven requests. deposits breach a"));
    }

    @Test
    public void testOrderMinDouble() {
        // with double
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(scan, Aggregate.MIN, DataType.DOUBLE, 6);

        DBColumn[] resultDouble = agg.execute();
        double outputDouble = resultDouble[0].getAsDouble()[0];
        assertTrue(outputDouble == 0.0);
    }

    @Test
    public void testOrderMinInt() {
        // with int
        ch.epfl.dias.ops.block.Scan scanInt = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.block.ProjectAggregate aggInt = new ch.epfl.dias.ops.block.ProjectAggregate(scanInt, Aggregate.MIN, DataType.INT, 0);
        
        DBColumn[] resultInt = aggInt.execute();
        int outputInt = resultInt[0].getAsInteger()[0];
        assertTrue(outputInt == 1);
    }

    @Test
    public void testOrderMinString() {
        // with string
        ch.epfl.dias.ops.block.Scan scanString = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
        ch.epfl.dias.ops.block.ProjectAggregate aggString = new ch.epfl.dias.ops.block.ProjectAggregate(scanString, Aggregate.MIN, DataType.STRING, 15);
        
        DBColumn[] resultString = aggString.execute();
        String outputString = resultString[0].getAsString()[0];
        assertTrue(outputString.equals("arefully slyly ex"));
    }

    @Test
    public void testProjectI() {
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(columnstoreOrder);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.block.Project projectOrder = new ch.epfl.dias.ops.block.Project(scanOrder, toProject);

        DBColumn[] result = projectOrder.execute();
        assertTrue(result[0].getAsInteger()[0] == 1);
    }

    @Test
    public void testProjectII() {
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(columnstoreData);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.block.Project projectOrder = new ch.epfl.dias.ops.block.Project(scanOrder, toProject);

        DBColumn[] result = projectOrder.execute();
        Integer[] column = result[0].getAsInteger();
        for (int i = 0; i < result[0].column.length; i++) {
            assertTrue(column[i] == i+1);
        }
    }

    @Test
    public void testSelectProject() {
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(columnstoreOrder);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.block.Project project = new ch.epfl.dias.ops.block.Project(scanOrder, toProject);
        ch.epfl.dias.ops.block.Select select = new ch.epfl.dias.ops.block.Select(project,BinaryOp.EQ, 0, 1);

        DBColumn[] result = select.execute();
        assertTrue(result[0].getAsInteger()[0] == 1);
    }

    @Test
    public void testSelectProjectBig() {
        ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(columnstoreOrderBig);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.block.Project project = new ch.epfl.dias.ops.block.Project(scanOrder, toProject);
        ch.epfl.dias.ops.block.Select select = new ch.epfl.dias.ops.block.Select(project,BinaryOp.EQ, 0, 1);

        DBColumn[] result = select.execute();
        assertTrue(result[0].getAsInteger()[0] == 1);
    }

    @Test
    public void testOrderMinDoubleBig() {
        // with double
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreLineItemBig);
        ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(scan, Aggregate.MIN, DataType.DOUBLE, 6);

        DBColumn[] resultDouble = agg.execute();
        double outputDouble = resultDouble[0].getAsDouble()[0];
        assertTrue(outputDouble == 0.0);
    }

	@Test
	public void joinTest1() {
		/*
		 * SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

		ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(columnstoreOrder);
		ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);

		/* Filtering on both sides */
		ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.block.Select selLineitem = new ch.epfl.dias.ops.block.Select(scanLineitem, BinaryOp.EQ, 0, 3);

		ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(selOrder, selLineitem, 0, 0);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

	@Test
	public void joinTest2() {
		/*
		 * SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey)
		 * WHERE orderkey = 3;
		 */

		ch.epfl.dias.ops.block.Scan scanOrder = new ch.epfl.dias.ops.block.Scan(columnstoreOrder);
		ch.epfl.dias.ops.block.Scan scanLineitem = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);

		/* Filtering on both sides */
		ch.epfl.dias.ops.block.Select selOrder = new ch.epfl.dias.ops.block.Select(scanOrder, BinaryOp.EQ, 0, 3);
		ch.epfl.dias.ops.block.Select selLineitem = new ch.epfl.dias.ops.block.Select(scanLineitem, BinaryOp.EQ, 0, 3);

		ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(selLineitem, selOrder, 0, 0);
		ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join, Aggregate.COUNT,
				DataType.INT, 0);

		DBColumn[] result = agg.execute();

		// This query should return only one result
		int output = result[0].getAsInteger()[0];

		assertTrue(output == 3);
	}

    @Test
    public void query1() {
        ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreLineItemBig);
        ch.epfl.dias.ops.block.Select projectAggregate = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.GE, 4, 15);
        int[] projection = {1,4};
        ch.epfl.dias.ops.block.Project project = new ch.epfl.dias.ops.block.Project(projectAggregate, projection);
        
        DBColumn[] result = project.execute();
        int index = 0;
        for (int i = 0; i < result[0].column.length; i++) {
            System.out.println(result[0].getAsInteger()[i]+ ", "+ result[1].getAsInteger()[i]);
            index++;
        }
        System.out.println(index);
    }
}