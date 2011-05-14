package io.s4.pig;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.lang.Exception;
import java.io.IOException;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.NoSuchMethodException;
import java.lang.ClassNotFoundException;

// S4
import io.s4.serialize.KryoSerDeser;

// Hadoop/Pig
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

/**
 * Decodes S4 events to read values. Relies on <tt>getValue</tt> type
 * methods.  Reading the <tt>class</tt> field is a special case that
 * returns the name of the class as a string.
 * <p>
 * Usage:
 * <p>
 * First, define a reference to the class with a given event type, for example:<br>
 * <pre>  define myEventReader io.s4.pig.S4EventReader('com.mycorp.mypkg.MyEvent');</pre>
 * <p>
 * The class can then be used to append values from events.  If we
 * have a bag of events in <tt>rawEvents</tt> as the (flattened)
 * output of {@link S4PEWrapper}, we can append the <tt>time</tt>
 * field:<br>
 * <pre>
 *  eventsWithTime =
 *  foreach
 *	rawEvents
 *  generate
 *	*,
 *	(long)speechTimeReader(event, 'time')#'time' as time;
 * </pre>
 */
public class S4EventReader extends EvalFunc<Map<Object, Object> >
{
    /**
     * Class constructor (argument required).
     * <p>
     * Loads event class methods via reflection to speed calling at
     * run-time. Also instantiates serializer.
     *
     * @param options  String with name of event class this instance will read.
     *
     */
    public S4EventReader(String options) throws Exception, ClassNotFoundException
    {
	super();

	if (options == null)
	    throw new Exception("Must provide event class name");

	String[] inputArgs = options.split("\\s*;\\s*");

	if (inputArgs.length < 1 || inputArgs[0] == null)
	    throw new Exception("Must provide event class name");

	// try to instantiate the event class
	eventName = inputArgs[0];
	Class eventClass = Class.forName(eventName);
	getters = new HashMap<String, Method>();

	Method[] allMethods = eventClass.getMethods();

	// add any public method starting with get to hashmap
	for (int i=0; i < allMethods.length; i++) {
	    Method m = allMethods[i];
	    String name = m.getName();

	    if (name.length() > 3 && name.substring(0,3).equals("get")
		&& (m.getModifiers() & Modifier.PUBLIC) != 0) {

		// only care about non-void primitives, Strings and
		// sub-classes of Number
		Class rt = m.getReturnType();
		if ( (rt.isPrimitive() && !rt.equals(Void.TYPE))
		     || rt.equals(String.class)
		     || rt.isInstance(Number.class) ) {

		    String fieldName = name.substring(3).toLowerCase();
		    getters.put(fieldName, m);
		}
	    }
	}

    }


    /**
     * Reads values from a given S4 event using getter methods.
     * <p>
     * All field names in the input tuple aside from the class name
     * must exactly match the name of a setter in the event.  For
     * example, an input field named "value" will call the setValue()
     * method when creating an event.
     * <p>
     * Getter name mismatches will print a stacktrace and return null.
     *
     * @param input  Tuple of arbitrary length. First element is the
     * serialized event. Other elements are strings giving the names
     * of the values to read.
     *
     * @return Map of values requested from the event.
     *
     */
    public Map<Object,Object> exec(Tuple input) throws IOException {
	if (input == null || input.size() < 2 || input.get(0) == null)
	    return null;

	// deserialize event
	Object event = serializer.deserialize( ((DataByteArray)input.get(0)).get());

	HashMap<Object,Object> outputMap = new HashMap<Object,Object>();

	// iterate through fields getting values
	for (int i=1; i < input.size(); i++) {

	    String fieldName = ((String)input.get(i)).toLowerCase();
	    if (fieldName == null)
		return null;

	    // get value, set in output map
	    Object value;
	    try {
		Method m = getters.get(fieldName);
		value = m.invoke(event);

		// if calling getClass, return class name as string
		if (fieldName.equalsIgnoreCase("class")) {
		    value = ((Class)value).getName();
		}
	    }
	    catch (Exception e) {
		e.printStackTrace();
		return null;
	    }

	    outputMap.put(fieldName, value);
	}

	return outputMap;
    }


    /**
     * Creates output schema (used internally by Pig).
     * 
     * @param input  Schema object of input
     *
     * @return
     *   Schema format:<br>
     *   <pre>    (result1, result2, ..., resultN)</pre>
     */
    public Schema outputSchema(Schema input) {
	return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.MAP));
    }

    // serializer
    private static KryoSerDeser serializer = new KryoSerDeser();

    // name of event class event class, hash of getter methods
    private String eventName;
    private HashMap<String, Method> getters;
}
