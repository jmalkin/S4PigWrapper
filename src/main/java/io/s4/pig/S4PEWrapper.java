package io.s4.pig;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;

import java.lang.NoSuchMethodException;
import java.lang.IllegalAccessException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Array;

import io.s4.pig.util.PigEmitter;

// S4
import io.s4.processor.EventAdvice;
//import io.s4.processor.PrototypeWrapper;
import io.s4.processor.ProcessingElement;
import io.s4.dispatcher.partitioner.KeyInfo;
import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.dispatcher.partitioner.DefaultHasher;
import io.s4.dispatcher.partitioner.DefaultPartitioner;
import io.s4.emitter.EventEmitter;
import io.s4.collector.EventWrapper;
import io.s4.serialize.KryoSerDeser;
//import io.s4.util.clock.Clock;
//import io.s4.util.clock.WallClock;
//import io.s4.util.clock.EventClock;
//import io.s4.util.clock.ClockStreamsLoader;

// Hadoop/Pig
import org.apache.pig.EvalFunc;
import org.apache.pig.Accumulator;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.PigException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

// Spring
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.RootBeanDefinition;

// TODO: Extend class to allow passing in a pre-serialized PE instance?
// (would actually be a derived class, not part of this one)

/**
 * Wraps S4 processing elements (PEs) to allow them to be called from
 * pig scripts.
 * <p>
 * Usage:
 * <p>
 * The PE wrapper is first declared, along with the class to wrap, the
 * path to the Spring config file, and the name of the config file, in
 * one semicolon-delimited string (due to Pig limitations):
 * <pre>  define myPEWrapper io.s4.pig.S4PEWrapper('com.mycorp.mypkg.MyPE; homes/user/project/; app_conf.xml');</pre>
 * <p>
 * Given a bag of events, each <tt>group</tt>ed by key values (all
 * events must also be part of a stream/key name pair read by this PE,
 * too), events can be sent to the PE:<br>
 * <pre>
 * groupedEvents = group rawEvents by (streamName, keyName, keyValue);
 *
 * PEoutput =
 * foreach
 *	groupedEvents
 * generate
 *	flatten( myPEWrapper(rawEvents) ) as (serialPE, peKey, BofE);
 * </pre>
 * The bag of events (BofE) is the set of events produced by the
 * associated serialized PE.  If the PEs are to be injected into a
 * running system later, the key value associated with each PE
 * instance is also returned.
 * <p>
 * If the output events will be used, they should be
 * <tt>flatten</tt>ed again and <tt>group</tt>ed to pass them into
 * another wrapped PE.
 */
public class S4PEWrapper extends EvalFunc<Tuple> implements Accumulator<Tuple>
{

    /**
     * Class constructor (argument required).
     * <p>
     * Instantiates PE and calls the constructor. Also captures the
     * type of message to send to the PE when given data.
     *
     * @param options
     *   Semicolon (;) separated string with PE class name, path
     *   to Spring XML config file, and config filename.
     *
     */
    public S4PEWrapper(String options) throws Exception
    {
	super();

	if (options == null)
	    throw new Exception("Must provide class name, stream name, and Spring XML file to S4PEWrapper");

	String[] inputArgs = options.split("\\s*;\\s*");

	if (inputArgs.length < 3 || inputArgs[0] == null || inputArgs[1] == null || inputArgs[2] == null )
	    throw new Exception("Must provide class name, stream name, and Spring XML file to S4PEWrapper");

	String springConfigPath = null;
	peBeanName       = inputArgs[0];
	springConfigPath = inputArgs[1];
	springConfigFile = inputArgs[2];

	copySpringConfigFile(springConfigPath, springConfigFile);
    }


    /**
     * Ensures the relevant Spring XML config file is in the right
     * places.
     * <p>
     * @param configPath  Base path (HDFS) of config file
     * @param configFile  Relative path to actual config file (path will be
     * replicated in job's local space)
     *
     */
    private void copySpringConfigFile(String configPath,
				      String configFile)
	throws IOException
    {
	if ((new File(configFile)).exists()) {
	    return;
	}

	// try to copy file into place
	try {
	    Configuration fscfg = new Configuration();
	    FileSystem hdfs = FileSystem.get(fscfg);

	    String hdfsPath = FileSystem.getDefaultUri(fscfg) + File.separator + configPath + File.separator + configFile;
	    Path configFileSrc = new Path(hdfsPath);
	    Path configFileDst = new Path(configFile);

	    // make target directory
	    // includes tree if needed, no error if already exists)
	    hdfs.mkdirs(configFileDst.getParent());

	    // TODO: determine if need to explicitly overwrite (or not)
	    hdfs.copyToLocalFile(configFileSrc, configFileDst);

	}
	catch (IOException e) {
	    System.err.println(e.getMessage());
	    throw new IOException("Could not copy Spring config file", e);
	}
    }

    /**
     * Instantiates the PE prototype from which specific instances are
     * cloned.  Also takes replaces the S4 comm layer, allowing pig to
     * capture output events.
     * <p>
     * @param peBeanName  Bean id of target PE (not the class name)
     * @param springConfigFile  Name of Spring XML config file
     *
     */
    private void buildPrototypePE(String peBeanName,
				  String springConfigFile)
    {
	// create our own hasher and override the emitter for Spring
	baseContext = new GenericApplicationContext();

	BeanDefinition hasher = new RootBeanDefinition(DefaultHasher.class);
	baseContext.registerBeanDefinition("hasher", hasher);

	BeanDefinition commEmitter = new RootBeanDefinition(PigEmitter.class);
	baseContext.registerBeanDefinition("commLayerEmitter", commEmitter);
	baseContext.refresh();

	// load the config file, extending the base context
	String[] configFileList = new String[] {"file:"+springConfigFile};

	context = new FileSystemXmlApplicationContext(configFileList, baseContext);
	emitter = (PigEmitter)context.getBean("commLayerEmitter");

	this.peBeanName = peBeanName;

	pePrototype = (ProcessingElement)context.getBean(peBeanName);
	keyInfoList = pePrototype.advise();
	//prototypeWrapper = new PrototypeWrapper( (ProcessingElement)context.getBean(peBeanName), clock);
	//keyInfoList = prototypeWrapper.advise();
    }


    /**
     * Validates input events, hands them off to a PE instance.
     * <p>
     * @param input
     *   Bag of tuples, each with 5 elements:
     *   <ol>
     *     <li>Stream name</li>
     *     <li>Key name</li>
     *     <li>Key value</li>
     *     <li>CompoundKeyInfo (serialized)</li>
     *     <li>Event (serialized)</li>
     *   </ol>
     * <p>
     * Key name/value may be null if PE reads * on that
     * stream. CompoundKeyInfo will also be null in such cases.
     *
     * @return Tuple of three items:
     *    <ol>
     *      <li>Serialized PE instance</li>
     *      <li>Key value associated with PE instance (as String)</li>
     *      <li>Bag of output event tuples, organized like input bag</li>
     *    </ol>
     *
     */
    public Tuple exec(Tuple input) throws IOException {
	if (input == null || input.size() < 1)
	    return null;

	try {
	    // clone the PE prototype
	    pe = getPEInstance();

	    // iterate through records (shared with accumulate)
	    processRecords( (DataBag)input.get(0) );

	    // use Accumulator interface function to
	    // handle gathering results
	    return createOutputTuple();

	} catch (Exception e) {
	    System.err.println("Error sending data to PE: " + e.getMessage());
	    e.printStackTrace();
	    return null;
	}

    }

    public void accumulate(Tuple input) throws IOException {
	if (input == null || input.size() < 1)
	    return;

	try {
	    // only clone the prototype PE if we don't have an instance
	    if (pe == null)
		pe = getPEInstance();

	    // iterate through records (shared with exec)
	    processRecords( (DataBag)input.get(0) );
	} catch (Exception e) {
	    System.err.println("Error sending data to PE: " + e.getMessage());
	    e.printStackTrace();
	    return;
	}
    }

    public void cleanup() {
	// Clear out the saved PE, so we'll get a new one next time
	keyValue = null;
	pe = null;
    }

    public Tuple getValue() {
	if (pe == null)
	    return null;

	try {
	    return createOutputTuple();
	} catch (PigException e) {
	    e.printStackTrace();
	    return null;
	}
    }

    /**
     * Returns a new PE instance, applying any needed initialization.
     *
     * @return a new ProcessingElement
     */
    private ProcessingElement getPEInstance() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
	if (baseContext == null || context == null)
	    buildPrototypePE(peBeanName, springConfigFile);

	// TODO: Should use prototypeWrapper.getPE(keyValue) --  need
	// to grabe the key value first, and possibly timestamp
	ProcessingElement newPE = (ProcessingElement)pePrototype.clone();

	// invoke PE instance initialization, if specified
	// (follows PrototypeWrapper.java)
	if (newPE.getInitMethod() != null) {
	    Method initMethod = newPE.getClass().getMethod(newPE.getInitMethod(), new Class[0]);
	    initMethod.invoke(newPE, (new Object[0]));
	}

	// TODO: instantiate S4Clock object as needed
	// pe.setClock()?

	keyValue = null;

	return newPE;
    }

    /**
     * Takes a PE instance and a bag of events, validates the input
     * events, and sends them to the PE for processing.  Output events
     * are sent to the emitter, so this method returns nothing.
     * <p>
     * @param pe
     *   Processing element instance
     * @param records
     *   Bag of tuples, each with 5 elements:
     *   <ol>
     *     <li>Stream name</li>
     *     <li>Key name</li>
     *     <li>Key value</li>
     *     <li>CompoundKeyInfo (serialized)</li>
     *     <li>Event (serialized)</li>
     *   </ol>
     * <p>
     * Key name/value may be null if PE reads * on that
     * stream. CompoundKeyInfo will also be null in such cases.
     */
    private void processRecords(DataBag records) throws Exception {
	// iterate through data records
	for (Iterator<Tuple> it = records.iterator(); it.hasNext();) {
	    Tuple eventTuple = (Tuple)it.next();

	    String streamName = (String)eventTuple.get(0);
	    String keyName = (String)eventTuple.get(1);
	    keyValue = (String)eventTuple.get(2);

	    // match stream name and key info against advice info
	    if (!validateEventInfo(streamName, keyName))
		throw new Exception(String.format("Invalid stream and key names for PE type! (PE: %s, provided stream: %s, provided key: %s", peBeanName, streamName, keyName));

	    // deserialize compoundKeyInfo and event, which retain original type
	    DataByteArray rawKeyInfo = (DataByteArray)eventTuple.get(3);
	    DataByteArray rawEvent = (DataByteArray)eventTuple.get(4);

	    CompoundKeyInfo keyInfo = null;
	    if (rawKeyInfo != null)
		keyInfo = (CompoundKeyInfo)serializer.deserialize( rawKeyInfo.get() );
	    Object event = serializer.deserialize( rawEvent.get() );

	    pe.execute(streamName, keyInfo, event);
	}
    }

    /**
     * Given stream and key names, checks that the PE accepts these
     * types of events. This method does not validate the key values,
     * only that this sort of event is appropriate for the current PE.
     * <p>
     * @param streamName Stream name
     * @param keyName Key field name (not value)
     *
     * @return <tt>true</tt> if the PE can accept this information,
     * <tt>false</tt> if not.
     *
     */
    private boolean validateEventInfo(String streamName, String keyName) {

	for (int i=0; i<keyInfoList.size(); i++) {
	    EventAdvice ea = keyInfoList.get(i);

	    if (ea.getEventName().equals(streamName)) {
		String targetKey = ea.getKey();

		if (targetKey.equals(keyName) || targetKey.equals("*"))
		    return true;
	    }
	}

	return false;
    }


    /**
     * Creates output schema (used internally by Pig).
     * 
     * @param input  Schema object of input
     *
     * @return
     *   Schema format:<br>
     *   <pre>    (serializedPE, PEKey, bag_of_events: {(stream, keyName, keyValue, compoundKeyInfo, event)})</pre>
     */
    public Schema outputSchema(Schema input) {
	try {
	    Schema tupleSchema = new Schema();

	    // add serialized PE, key value
	    tupleSchema.add(new Schema.FieldSchema("serializedPE", DataType.CHARARRAY));
	    tupleSchema.add(new Schema.FieldSchema("PEKey", DataType.CHARARRAY));

	    // bag of event tuples (key, bytearray)
	    Schema eventTuple = new Schema();
	    eventTuple.add(new Schema.FieldSchema("stream", DataType.CHARARRAY));
	    eventTuple.add(new Schema.FieldSchema("keyName", DataType.CHARARRAY));
	    eventTuple.add(new Schema.FieldSchema("keyValue", DataType.CHARARRAY));
	    eventTuple.add(new Schema.FieldSchema("compoundKeyInfo", DataType.BYTEARRAY));
	    eventTuple.add(new Schema.FieldSchema("event", DataType.BYTEARRAY));

	    Schema.FieldSchema bagSchema = new Schema.FieldSchema("bag_of_events", eventTuple, DataType.BAG);
	    tupleSchema.add(bagSchema);

	    // return schema for row based on name of udf/method
	    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), tupleSchema, DataType.TUPLE));
	}
	catch (Exception e) {
	    return null;
	}
    }


    /**
     * Creates the final output Tuple, with the serialized PE, key
     * value for that PE instance, and the bag of output events.
     * <p>
     * @return (serialized_PE, PE_key, bag_of_events) (see {@link
     * outputSchema})
     */
    private Tuple createOutputTuple() throws PigException {
	// Create a tuple to return
	Tuple outputTuple = tupleFactory.newTuple(3);

	// TODO: serialize PE rather than using toString()
	String PEstring = null;
	if (pe != null)
	    PEstring = "pe:" + pe.toString();
	DataBag eventBag = createEventDataBag();

	outputTuple.set(0, PEstring);
	outputTuple.set(1, keyValue);
	outputTuple.set(2, eventBag);

	return outputTuple;
    }

    /**
     * Creates a bag of events to return. Loads data from the
     * PigEmitter.
     * <p>
     * @return Bag of events using format from <tt>outputSchema</tt>
     * (see {@link outputSchema})
     *
     */
    private DataBag createEventDataBag() throws PigException {

	DataBag eventBag = bagFactory.newDefaultBag();

	if (emitter == null || emitter.eventCount() == 0)
	    return eventBag;

	ArrayList<EventWrapper> eventList = emitter.flushEvents();

	for (Iterator it = eventList.iterator(); it.hasNext();) {
	    EventWrapper wrappedEvent = (EventWrapper)it.next();

	    String className = wrappedEvent.getEvent().getClass().getName();
	    byte[] rawEvent = serializer.serialize(wrappedEvent.getEvent());
	    DataByteArray eventByteArray = new DataByteArray(rawEvent);

	    // Break apart CompoundKeyInfo list into separate CompoundKeyInfo objects.
	    // Add one tuple to the bag for each result.
	    List<CompoundKeyInfo> keyInfoList = wrappedEvent.getCompoundKeys();
	    for (int i=0; i<keyInfoList.size(); i++) {
		Tuple eventTuple = tupleFactory.newTuple(5);

		CompoundKeyInfo keyInfo = keyInfoList.get(i);
		byte[] rawKeyInfo = serializer.serialize(keyInfo);
		DataByteArray keyInfoByteArray = new DataByteArray(rawKeyInfo);

		eventTuple.set(0, wrappedEvent.getStreamName());
		eventTuple.set(1, keyInfo.getCompoundKey());
		eventTuple.set(2, keyInfo.getCompoundValue());
		eventTuple.set(3, keyInfoByteArray);
		eventTuple.set(4, eventByteArray);

		eventBag.add(eventTuple);
	    }
	}

	return eventBag;
    }

    // PE we're wrapping and associated info
    private String springConfigFile;
    private String peBeanName;
    private ProcessingElement pePrototype;
    //private PrototypeWrapper prototypeWrapper;
    //private Clock clock;

    // Needed for Accumulator interface
    private ProcessingElement pe;
    private String keyValue;

    // Emitter to override the default CommLayerEmitter
    private PigEmitter emitter;

    // Sping application info -- static to avoid many
    // application creation calls
    private GenericApplicationContext baseContext;
    private AbstractApplicationContext context;

    // Advice info
    private List<EventAdvice> keyInfoList;

    // Serializer
    private static KryoSerDeser serializer = new KryoSerDeser();

    // Factories we'll use to speed calls
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory   bagFactory   = BagFactory.getInstance();
}
