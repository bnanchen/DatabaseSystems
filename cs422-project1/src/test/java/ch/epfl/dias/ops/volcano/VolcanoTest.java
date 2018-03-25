package ch.epfl.dias.ops.volcano;

import static org.junit.Assert.*;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

import org.junit.Before;
import org.junit.Test;

public class VolcanoTest {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;
    
    RowStore rowstoreData;
    RowStore rowstoreOrder;
    RowStore rowstoreLineItem;
	RowStore rowstoreOrderBig;
	RowStore rowstoreLineItemBig;
    
    @Before
    public void init()  {
    	
		schema = new DataType[]{ 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT,
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT, 
				DataType.INT };
    	
        orderSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.STRING,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.INT,
                DataType.STRING};

        lineitemSchema = new DataType[]{
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.INT,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.DOUBLE,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING,
                DataType.STRING};
        
        rowstoreData = new RowStore(schema, "input/data.csv", ",");
        rowstoreData.load();
        
        rowstoreOrder = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
        rowstoreOrder.load();
        
        rowstoreLineItem = new RowStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        rowstoreLineItem.load();

        rowstoreOrderBig = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
        rowstoreOrderBig.load();

        rowstoreLineItemBig = new RowStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
        rowstoreLineItemBig.load();
    }
    
	@Test
	public void spTestData(){
	    /* SELECT COUNT(*) FROM data WHERE col4 == 6 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 3, 6);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 3);
	}
	
	@Test
	public void spTestOrder(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 6 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 6);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 1);
	}
	
	@Test
	public void spTestLineItem(){
	    /* SELECT COUNT(*) FROM data WHERE col0 == 3 */	    
	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	    ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 0, 3);
	    ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		// This query should return only one result
		DBTuple result = agg.next();
		int output = result.getFieldAsInt(0);
		assertTrue(output == 3);
	}

	@Test
    public void testOrderSumInt() {
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, 0);

        agg.open();

        DBTuple result = agg.next();
        int output = result.getFieldAsInt(0);
        assertTrue(output == 17);
    }

    @Test
    public void testOrderSumDouble() {
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.SUM, DataType.DOUBLE, 5);

        agg.open();

        DBTuple result = agg.next();
        double output = result.getFieldAsDouble(0);
        assertTrue(output == 454890.80);
    }

    @Test
    public void testOrderAvg() {
        // with double
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 5);

        // with int
        ch.epfl.dias.ops.volcano.Scan scan_ = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg_ = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan_, Aggregate.AVG, DataType.INT, 0);


        agg.open();

        DBTuple result = agg.next();
        DBTuple result_ = agg_.next();
        double output = result.getFieldAsDouble(0);
        int output_ = result_.getFieldAsInt(0);
        assertTrue(output == 45489.08);
        assertTrue(output_ == 1);

    }

    @Test
    public void testOrderMaxDouble() {
        // with double
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MAX, DataType.DOUBLE, 6);

        agg.open();

        DBTuple resultDouble = agg.next();
        double outputDouble = resultDouble.getFieldAsDouble(0);
        assertTrue(outputDouble == 0.10);
    }

    @Test
    public void testOrderMaxInt() {
        // with int
        ch.epfl.dias.ops.volcano.Scan scanInt = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate aggInt = new ch.epfl.dias.ops.volcano.ProjectAggregate(scanInt, Aggregate.MAX, DataType.INT, 0);

        aggInt.open();

        DBTuple resultInt = aggInt.next();
        int outputInt = resultInt.getFieldAsInt(0);
        assertTrue(outputInt == 3);
    }

    @Test
    public void testOrderMaxString() {
        // with string
        ch.epfl.dias.ops.volcano.Scan scanString = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate aggString = new ch.epfl.dias.ops.volcano.ProjectAggregate(scanString, Aggregate.MAX, DataType.STRING, 15);

        aggString.open();

        DBTuple resultString = aggString.next();
        String outputString = resultString.getFieldAsString(0);
        assertTrue(outputString.equals("ven requests. deposits breach a"));
    }

    @Test
    public void testOrderMinDouble() {
        // with double
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MIN, DataType.DOUBLE, 6);

        agg.open();

        DBTuple resultDouble = agg.next();
        double outputDouble = resultDouble.getFieldAsDouble(0);
        assertTrue(outputDouble == 0.0);
    }

    @Test
    public void testOrderMinInt() {
        // with int
        ch.epfl.dias.ops.volcano.Scan scanInt = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate aggInt = new ch.epfl.dias.ops.volcano.ProjectAggregate(scanInt, Aggregate.MIN, DataType.INT, 0);

        aggInt.open();

        DBTuple resultInt = aggInt.next();
        int outputInt = resultInt.getFieldAsInt(0);
        assertTrue(outputInt == 1);
    }

    @Test
    public void testOrderMinString() {
        // with string
        ch.epfl.dias.ops.volcano.Scan scanString = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
        ch.epfl.dias.ops.volcano.ProjectAggregate aggString = new ch.epfl.dias.ops.volcano.ProjectAggregate(scanString, Aggregate.MIN, DataType.STRING, 15);

        aggString.open();

        DBTuple resultString = aggString.next();
        String outputString = resultString.getFieldAsString(0);
        System.out.println(outputString);
        assertTrue(outputString.equals("arefully slyly ex"));
    }

    @Test
    public void testProjectI() {
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.volcano.Project projectOrder = new ch.epfl.dias.ops.volcano.Project(scanOrder, toProject);

        projectOrder.open();

        DBTuple result = projectOrder.next();
        assertTrue(result.getFieldAsInt(0) == 1);
    }

    @Test
    public void testProjectII() {
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreData);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.volcano.Project projectOrder = new ch.epfl.dias.ops.volcano.Project(scanOrder, toProject);

        projectOrder.open();

        DBTuple result = projectOrder.next();
        int index = 1;
        while(!result.eof) {
            assertTrue(result.getFieldAsInt(0) == index);
            result = projectOrder.next();
            index++;
        }
    }

    @Test
    public void testSelectProject() {
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.volcano.Project project = new ch.epfl.dias.ops.volcano.Project(scanOrder, toProject);
        ch.epfl.dias.ops.volcano.Select select = new ch.epfl.dias.ops.volcano.Select(project,BinaryOp.EQ, 0, 1);

        select.open();

        DBTuple result = select.next();
        assertTrue(result.getFieldAsInt(0) == 1);
    }

    @Test
    public void testSelectProjectBig() {
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrderBig);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.volcano.Project project = new ch.epfl.dias.ops.volcano.Project(scanOrder, toProject);
        ch.epfl.dias.ops.volcano.Select select = new ch.epfl.dias.ops.volcano.Select(project,BinaryOp.EQ, 0, 1);

        select.open();

        DBTuple result = select.next();
        assertTrue(result.getFieldAsInt(0) == 1);
    }

    @Test
    public void testOrderMinDoubleBig() {
        // with double
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItemBig);
        ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.MIN, DataType.DOUBLE, 6);

        agg.open();

        DBTuple resultDouble = agg.next();
        double outputDouble = resultDouble.getFieldAsDouble(0);
        assertTrue(outputDouble == 0.0);
    }


	@Test
	public void joinTest1(){
	    /* SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	
	    /*Filtering on both sides */
	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
	
	    HashJoin join = new HashJoin(selOrder,selLineitem,0,0);
	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBTuple result = agg.next();
	    int output = result.getFieldAsInt(0);
	    assertTrue(output == 3);
	}
	
	@Test
	public void joinTest2(){
	    /* SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey) WHERE orderkey = 3;*/
	
		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
	
	    /*Filtering on both sides */
	    Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
	    Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);
	
	    HashJoin join = new HashJoin(selLineitem,selOrder,0,0);
	    ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);
	
	    agg.open();
	    //This query should return only one result
	    DBTuple result = agg.next();
	    int output = result.getFieldAsInt(0);
	    assertTrue(output == 3);
	}

	@Test
    public void query1() {
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItemBig);
        ch.epfl.dias.ops.volcano.Select projectAggregate = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, 4, 15);
        int[] projection = {1,4};
        ch.epfl.dias.ops.volcano.Project project = new ch.epfl.dias.ops.volcano.Project(projectAggregate, projection);

        project.open();
        DBTuple result = project.next();
        while(!result.eof) {
            result = project.next();
        }
        project.close();
    }

    @Test
    public void query2() {
        ch.epfl.dias.ops.volcano.Scan scanLine = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItemBig);
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrderBig);
        int[] projection = {0};
        ch.epfl.dias.ops.volcano.Project projectLine = new ch.epfl.dias.ops.volcano.Project(scanLine, projection);
        ch.epfl.dias.ops.volcano.Project projectOrder = new ch.epfl.dias.ops.volcano.Project(scanOrder, projection);
        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(projectLine, projectOrder, 0, 0);

        join.open();

        DBTuple result = join.next();
        while(!result.eof) {
            result = join.next();
        }
        join.close();
    }

    @Test
    public void query3() {
        ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItemBig);
        ch.epfl.dias.ops.volcano.Select projectAggregate = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, 4, 15);
        int[] projection = {1,4};
        ch.epfl.dias.ops.volcano.Project project = new ch.epfl.dias.ops.volcano.Project(projectAggregate, projection);
        ch.epfl.dias.ops.volcano.ProjectAggregate pj = new ch.epfl.dias.ops.volcano.ProjectAggregate(project, Aggregate.COUNT, DataType.INT,  0);

        pj.open();
        DBTuple result = pj.next();
        while(!result.eof) {
            result = pj.next();
        }
        pj.close();
    }

    @Test
    public void query4() {
        ch.epfl.dias.ops.volcano.Scan scanLine = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItemBig);
        ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrderBig);
        int[] projectionLine = {0, 13};
        int[] projectionOrder = {0};
        ch.epfl.dias.ops.volcano.Project projectLine = new ch.epfl.dias.ops.volcano.Project(scanLine, projectionLine);
        ch.epfl.dias.ops.volcano.Project projectOrder = new ch.epfl.dias.ops.volcano.Project(scanOrder, projectionOrder);
        ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(projectLine, projectOrder, 0, 0);
        ch.epfl.dias.ops.volcano.ProjectAggregate pj = new ch.epfl.dias.ops.volcano.ProjectAggregate(join, Aggregate.MAX, DataType.STRING, 1);

        pj.open();

        DBTuple result = pj.next();
        while(!result.eof) {
            System.out.println(result.fields[0]);
            result = pj.next();
        }
        pj.close();
    }
}
