package com.example.hive;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class SumArray extends GenericUDF {

	ListObjectInspector listio;
	
	/*Initialize function and return ObjectInspector of Primitive type based on
	 * element datatype.
	 * 
	 */
	public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		if ( (args.length != 1) || !(args[0] instanceof ListObjectInspector)){
			throw new UDFArgumentException("This Function only takes one argument of List type");
		}
		this.listio = (ListObjectInspector) args[0];
		//Test to check the element datatype
		if (listio.getListElementObjectInspector() instanceof DoubleObjectInspector){
			return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
		}
		else if (listio.getListElementObjectInspector() instanceof IntObjectInspector){
			return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		}
		else if (listio.getListElementObjectInspector() instanceof LongObjectInspector){
			return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
		}
		else if (listio.getListElementObjectInspector() instanceof FloatObjectInspector){
			return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
		}
		else {
			throw new UDFArgumentException("List elements have to be of numeric type");
		}
	}
	//Don't know the purpose of this.
	public String getDisplayString(String[] args){
		return "Sum of all list elements!"; //hope it's fine
	}
	
	//UDF logic goes here
	@SuppressWarnings("unchecked")
	public Object evaluate(DeferredObject[] elements) throws HiveException{
		
		//See if its an null list, or List[]
		if (listio.getListLength(elements[0].get()) == 0){
			return null;
		}
		//Process Doubles
		else if (listio.getListElementObjectInspector() instanceof DoubleObjectInspector){
			Double sum = 0.0;
			List<Double> doubles = (List<Double>) this.listio.getList(elements[0].get());
			for(Double d: doubles){
				sum += d;
			}
			return new Double(sum);
		}
		//Floats
		else if (listio.getListElementObjectInspector() instanceof FloatObjectInspector){
			Float sum = 0.0f;
			List<Float> floats = (List<Float>) this.listio.getList(elements[0].get());
			
			for (Float f: floats){
				sum += f;
			}
			return new Float(sum);
		}
		//Integers
		else if (listio.getListElementObjectInspector() instanceof IntObjectInspector){
			Integer sum = 0;
			List<Integer> ints = (List<Integer>) this.listio.getList(elements[0].get());
			
			for (Integer i: ints){
				sum += i;
			}
			
			return new Integer(sum);
		}
		//Longs
		else if (listio.getListElementObjectInspector() instanceof LongObjectInspector){
			Long sum = 0L;
			List<Long> longs = (List<Long>) this.listio.getList(elements[0].get());
			
			for (Long l: longs){
				sum += l;
			}
			return new Long(sum);
		}
		else {
			//else
			return null;
		}
	}
}
