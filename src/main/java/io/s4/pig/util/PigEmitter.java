package io.s4.pig.util;

import java.util.ArrayList;

import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.nio.ByteBuffer;

import java.io.IOException;
import java.io.FileNotFoundException;

import io.s4.emitter.EventEmitter;
import io.s4.collector.EventWrapper;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;


/**
 * Override S4's default CommLayerEmitter, allowing output events from
 * each PE instance to be collected and written back out to pig for
 * redirecting.
 * <p>
 * Will eventually cache events to local storage, reducing the memory
 * footprint while running.
 */
public class PigEmitter implements EventEmitter
{
    // Event list for small numbers of events
    private ArrayList<EventWrapper> eventList = null;

    // Info for caching to disk
    private int eventCount = 0;
    private File eventCache = null;
    private OutputStream cacheOS = null;
    private int maxMemoryEvents = 1048575; // 2^20 - 1

    // Serializer (like io.s4.serialize.KryoSerDeser, but
    // with some frequently used classes registered)
    private PigKryoSerDeser serializer = new PigKryoSerDeser();

    /**
     * Sets the number of events stored in memory before caching to disk.
     */
    public void setMaxMemoryEvents(int maxMemoryEvents) {
	this.maxMemoryEvents = maxMemoryEvents;
    }

    /**
     * Returns the maximum number of events stored in memory before writing them to disk.
     *
     * @return The maximum allowed number of events in memory
     */
    public int getMaxMemoryEvents() {
	return maxMemoryEvents;
    }

    /**
     * Returns number of output events saved.
     *
     * @return The output event queue size
     */
    public int eventCount() {
	return eventCount;
    }

    /**
     * Used internally by S4 to signal to the emitter to emit wrapped
     * event on a given partition.  Here, just enqueues events.
     *
     * @param partitionId  Index number of partition to use (unused here)
     * @param eventWrapper  Wrapped event to emit
     */
    public void emit(int partitionId, EventWrapper eventWrapper) {
	if (eventCount < maxMemoryEvents) {
	    if (eventList == null) {
		eventList = new ArrayList<EventWrapper>();
	    }

	    eventList.add(eventWrapper);
	}
	else {
	    if (eventCache == null) {
		try {
		    eventCache = File.createTempFile("eventCache-", ".dat");
		    cacheOS = new BufferedOutputStream(new FileOutputStream(eventCache));
		} catch (IOException e) {
		    e.printStackTrace();
		    return;
		}

		// write events in memory to file first
		for (EventWrapper ew : eventList)
		    writeToEventCache(ew);
		eventList = null;
	    }

	    writeToEventCache(eventWrapper);
	}

	++eventCount;
    }

    private boolean writeToEventCache(EventWrapper eventWrapper) {
	if (eventCache == null)
	    return false;

	byte[] b = serializer.serialize(eventWrapper);

	ByteBuffer bb = ByteBuffer.allocate(4);
	bb.putInt(b.length);

	try {
	    cacheOS.write(bb.array());
	    cacheOS.write(b);
	} catch (IOException e) {
	    e.printStackTrace();
	    return false;
	}

	return true;
    }

    private ArrayList<EventWrapper> loadEventCache() {
	// read events from file
	ArrayList<EventWrapper> events = new ArrayList<EventWrapper>();
	events.ensureCapacity(eventCount);

	BufferedInputStream is = null;
	try {
	    is = new BufferedInputStream(new FileInputStream(eventCache));
	    byte[] sizeBuf = new byte[4];

	    while (is.read(sizeBuf, 0, 4) != -1) {
		int size = ByteBuffer.wrap(sizeBuf).getInt();
		if (size < 1) {
		    System.err.println("PigEmitter: Invalid size reading events: " + size);
		    return null;
		}

		byte[] eventBuf = new byte[size];
		int bytesRead = is.read(eventBuf, 0, size);
		if (bytesRead < size) {
		    System.err.println(String.format("PigEmitter: Malformed event when reading event cache. Found %d bytes, expected %d.", bytesRead, size));
		    return null;
		}

		EventWrapper ew = (EventWrapper)serializer.deserialize(eventBuf);
		events.add(ew);
	    }
	}
	catch (IOException e) {
	    e.printStackTrace();
	    return null;
	}
	finally {
	    if (is != null) 
		try { is.close(); }	catch (Exception e) { }
	}

	// clean up after ourselves
	eventCache.delete();

	return events;
    }

    /**
     * Returns the number of nodes in the S4 cluster that this emitter knows (unused here).
     *
     * @return 1 (only 1 node matters in a pig context)
     */
    public int getNodeCount() {
	return 1;
    }

    /**
     * Returns all enqueued events and flushes the queue. Called by
     * S4PEWrapper before returning the bag of events.
     *
     * @return An ArrayList of EventWrappers.
     */
    public ArrayList<EventWrapper> flushEvents() {
	if (eventCount == 0)
	    return null;
	else if (eventList != null) {
	    // grab the event list and return that
	    ArrayList<EventWrapper> retList = eventList;
	    eventList = null;
	    eventCount = 0;
	    return retList;
	}
	else {
	    // close OutputStream
	    try {
		if (cacheOS != null)
		    cacheOS.close();
	    } catch (IOException e) {
		e.printStackTrace();
	    }

	    ArrayList<EventWrapper> retList = loadEventCache();
	    if (retList.size() != eventCount) {
		System.err.println(String.format("PigEmitter: Wrong number of events in file. Found %d, expected %d.", retList.size(), eventCount));
		return null;
	    }

	    eventCache = null;
	    eventCount = 0;
	    return retList;
	}
    }

    private class PigKryoSerDeser {
	private Kryo kryo = new Kryo();

	private PigKryoSerDeser() {
	    kryo.setRegistrationOptional(true);

	    // register some types we know are often used, as well as
	    // some generic S4 event-related classes
	    kryo.register(java.util.ArrayList.class);
	    kryo.register(java.util.HashMap.class);
	    kryo.register(io.s4.collector.EventWrapper.class);
	    kryo.register(io.s4.dispatcher.partitioner.CompoundKeyInfo.class);
	    kryo.register(io.s4.dispatcher.partitioner.KeyInfo.class);
	    kryo.register(io.s4.dispatcher.partitioner.KeyInfo.KeyPathElementName.class);
	}

	public Object deserialize(byte[] rawMessage) {
	    ObjectBuffer buffer = new ObjectBuffer(kryo);
	    return buffer.readClassAndObject(rawMessage);
	}

	public byte[] serialize(Object message) {
	    ObjectBuffer buffer = new ObjectBuffer(kryo);
	    return buffer.writeClassAndObject(message);
	}
    }
}
