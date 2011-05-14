package io.s4.pig;

import java.util.Iterator;
import java.util.HashMap;
import java.util.ArrayList;
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
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

/**
 * Builds (keyless) S4 events based on data supplied from pig.
 * <p>
 * Usage:
 * <p>
 * A reference to the event builder is defined, along with any field
 * names that will be set, all in a semicolon-separated string:<br>
 * <pre>  define craeteMyEvent io.s4.pig.S4EventBuilder('com.mycorp.mypkg.MyEvent; id; value; time');</pre>
 * <p>

 * If we have pig data stored in <tt>rawData</tt>, the most
 * straightforward usage nests the call to build events inside a
 * larger pig generate statement:
 * <pre>
 * rawSpeechEvts =
 * foreach
 *	speech_data
 * generate
 *	'RawSpeech' as streamName,
 *	null as keyName,
 *	null as keyValue,
 *	null as compoundKeyInfo,
 *	createSpeechEvent(id, location, speaker, time) as event;
 * </pre>

 * This creates events similar to a flattened bag of events (see
 * {@link S4PEWrapper}), although with no key informaiton.
 */
public class S4EventBuilder extends EvalFunc<DataByteArray>
{
    /**
     * Class constructor (argument required).
     * <p>
     * Loads method objects via reflection to speed calling at
     * run-time. Also instantiates serializer.
     *
     * @param options  Semicolon (;) separated string with event class
     * name and the names of fields <em>in the order in which they
     * will appear in any input tuples</em>.
     *
     */
    public S4EventBuilder(String options) throws Exception, ClassNotFoundException
    {
	super();

	if (options == null)
	    throw new Exception("Must provide class name, and names of input fields in the order they will appear in tuples");

	String[] inputArgs = options.split("\\s*;\\s*");

	if (inputArgs.length < 2 || inputArgs[0] == null || inputArgs[1] == null)
	    throw new Exception("Must provide class name, and names of input fields in the order they will appear in tuples");

	// try to instantiate the event class
	eventName = inputArgs[0];
	eventClass = Class.forName(eventName);
	methods = new ArrayList<MethodNamePair>(inputArgs.length-1);

	Method[] allMethods = eventClass.getMethods();

	// save public methods starting with set and matching input
	// field names into (order-preserving)
	for (int i=1; i < inputArgs.length; i++) {

	    String name = inputArgs[i];
	    if (name.length() < 1)
		throw new Exception("Empty field name requested");

	    String setterName = "set" + name;

	    for (int j=0; j < allMethods.length; j++) {

		Method m = allMethods[j];
		if (setterName.equalsIgnoreCase( m.getName() )
		    && (m.getModifiers() & Modifier.PUBLIC) != 0) {

		    MethodNamePair pair = new MethodNamePair();
		    pair.name   = name;
		    pair.method = m;
		    methods.add(pair);
		}
	    }
	}

    }


    /**
     * Creates a serialized S4 event given Pig data.
     * <p>
     * All field names in the input tuple must match the name of a
     * setter method in the event.  For example, an input field named
     * "value" will invoke the <tt>setValue</tt> method when creating
     * an event. Setters are always called in the order specified in
     * the constructor.  A <tt>null</tt> value means the setter for
     * that field is not called.
     * <p>
     * Type mismatches will produce an exception. Differences in case
     * are ignored.
     *
     * @param input  Tuple of values for each field, in the order provided to
     * the constructor.
     *
     * @return Serialized version of the event.
     */
    public DataByteArray exec(Tuple input) throws IOException {
	if (input == null || input.size() < methods.size())
	    return null;

	// create empty event object
	Object event;
	try {
	    event = eventClass.newInstance();
	}
	catch (Exception e) {
	    e.printStackTrace();
	    return null;
	}

	// iterate through fields setting values
	for (int i=0; i < methods.size(); i++) {
	    if (input.get(i) != null) {

		MethodNamePair pair = methods.get(i);
		try {
		    Method m = pair.method;
		    m.invoke(event, input.get(i));
		}
		catch (Exception e) {
		    e.printStackTrace();
		    return null;
		}
	    }
	}

	// serialize event
	Tuple outputTuple = tupleFactory.newTuple(2);
	byte[] rawEvent = serializer.serialize(event);
	DataByteArray serializedEvent = new DataByteArray(rawEvent);

	return serializedEvent;
    }

    /**
     * Creates output schema (used internally by Pig).
     * 
     * @param input  Schema object of input
     *
     * @return
     *   Schema format:<br>
     *   <pre>    (event: bytearray)</pre>
     */
    public Schema outputSchema(Schema input) {
	try {
	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.BYTEARRAY));
	}
	catch (Exception e) {
	    return null;
	}
    }

    // auxiliary class for storing (name, method) tuples
    private class MethodNamePair {
	public String name;
	public Method method;
    }

    // Serializer
    private static KryoSerDeser serializer = new KryoSerDeser();

    // name of event class event class, hash of setter methods
    private String eventName;
    private Class eventClass;
    private ArrayList<MethodNamePair> methods;

    // Factories we'll use to speed calls
    TupleFactory tupleFactory = TupleFactory.getInstance();
}
