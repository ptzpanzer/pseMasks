package main;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fraunhofer.iosb.ilt.frostserver.messagebus.MessageBusFactory;
import de.fraunhofer.iosb.ilt.frostserver.model.EntityType;
import de.fraunhofer.iosb.ilt.frostserver.model.Thing;
import de.fraunhofer.iosb.ilt.frostserver.model.core.Entity;
import de.fraunhofer.iosb.ilt.frostserver.model.core.EntitySetImpl;
import de.fraunhofer.iosb.ilt.frostserver.model.core.Id;
import de.fraunhofer.iosb.ilt.frostserver.model.core.IdString;
import de.fraunhofer.iosb.ilt.frostserver.parser.path.PathParser;
import de.fraunhofer.iosb.ilt.frostserver.parser.query.QueryParser;
import de.fraunhofer.iosb.ilt.frostserver.path.PathElementEntity;
import de.fraunhofer.iosb.ilt.frostserver.path.ResourcePath;
import de.fraunhofer.iosb.ilt.frostserver.path.Version;
import de.fraunhofer.iosb.ilt.frostserver.persistence.pgjooq.imp.PostgresPersistenceManagerString;
import de.fraunhofer.iosb.ilt.frostserver.query.Query;
import de.fraunhofer.iosb.ilt.frostserver.settings.CoreSettings;
import de.fraunhofer.iosb.ilt.frostserver.util.exception.IncompleteEntityException;
import de.fraunhofer.iosb.ilt.frostserver.util.exception.NoSuchEntityException;

public class MainMask {
	/**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MainMask.class);
    
    private static final String KEY_TEMP_PATH = "tempPath";
    private static final String CONFIG_FILE_NAME = "Mask.properties";
    
    private PostgresPersistenceManagerString ppm;
    private CoreSettings coreSettings;
    private MessageBusFactory messageBusFactory;
    
    
    public MainMask() throws IOException {
    	String configFileName = "src/" + CONFIG_FILE_NAME;
    	this.coreSettings = loadCoreSettings(configFileName);
		this.ppm = new PostgresPersistenceManagerString();
		this.ppm.init(coreSettings);
		MessageBusFactory.init(coreSettings);
    }
 

	private CoreSettings loadCoreSettings(String configFileName) throws IOException {
        Properties defaults = new Properties();
        defaults.setProperty(KEY_TEMP_PATH, System.getProperty("java.io.tmpdir"));
        Properties properties = new Properties(defaults);
        try (FileInputStream input = new FileInputStream(configFileName)) {
            properties.load(input);
            LOGGER.info("Read {} properties from {}.", properties.size(), configFileName);
        } catch (IOException exc) {
            LOGGER.info("Could not read properties from file: {}.", exc.getMessage());
        }
        
        return new CoreSettings(properties);
    }
	
	
	private Entity<?> getEntityById(EntityType et, String Id) {
		IdString idString = new IdString(Id);
		Entity<?> rtn = ppm.get(et, idString);

		return rtn;
	}
	
	
	private EntitySetImpl<?> getEntityByQuery(EntityType et, String queryString) {
		String entityPath = "/" + et.plural;
		ResourcePath rp = PathParser.parsePath("", Version.V_1_0, entityPath);
		Query query = QueryParser.parseQuery(queryString, coreSettings, rp);
		
		EntitySetImpl<?> rtn = (EntitySetImpl<?>) ppm.get(rp, query);
		return rtn;
	}
	
	
	private boolean insert(Entity entity) throws NoSuchEntityException, IncompleteEntityException {
		boolean rtn = ppm.insert(entity);
		return rtn;
	}
	
	
	private boolean update(Entity entity, Id id) throws NoSuchEntityException, IncompleteEntityException {
		PathElementEntity pathElement = new PathElementEntity(id, entity.getEntityType(), null);
		
		boolean rtn = ppm.update(pathElement, entity);
		
		return rtn;
	}
	
	
	private boolean delete(EntityType entityType, Id id) throws NoSuchEntityException {
		PathElementEntity pathElement = new PathElementEntity(id, entityType, null);
		
		boolean rtn = ppm.delete(pathElement);
		
		return rtn;
	}


	
	public static void main(String[] args) throws IOException, NoSuchEntityException, IncompleteEntityException, InterruptedException {
		MainMask mmsk = new MainMask();
		
		
		// Get an entity with EntityType and ID.
		Thing th = (Thing) mmsk.getEntityById(EntityType.THING, "saqn:t:grimm-aerosol.com:edm80opc:sn19001");
		System.out.println("------------------------------------");
		System.out.println(th.getName() + " , " + th.getDescription());
		
		System.out.println("------------------------------------");
		Thing th2 = (Thing) mmsk.getEntityById(EntityType.THING, "saqn:t:grimm-aerosol.com:edm80opc:sn19004");
		System.out.println(th2.getName() + " , " + th2.getDescription());
		
		
		// Create and insert
		Thing myThing1 = new Thing();
		myThing1.setName("myThing_1");
		myThing1.setDescription("Description for myThing_1");
		boolean success = mmsk.insert(myThing1);
		System.out.println("------------------------------------");
		System.out.println("Try create and insert.");
		System.out.format("Inserting Thing with id \"%s\".\n", myThing1.getId().toString());
		
		Thing myThing2 = new Thing();
		myThing2.setName("myThing_2");
		myThing2.setDescription("Description for myThing_2");
		success = mmsk.insert(myThing2);
		System.out.format("Inserting Thing with id \"%s\".\n", myThing2.getId().toString());
		
		Thing myThing3 = new Thing();
		myThing3.setName("myThing_3");
		myThing3.setDescription("Description for myThing_3");
		success = mmsk.insert(myThing3);
		System.out.format("Inserting Thing with id \"%s\".\n", myThing3.getId().toString());
		
		mmsk.ppm.commit();
		
		// Output result using Get
		String queryString = "$count=true&$orderby=@iot.id asc";
		EntitySetImpl entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.THING, queryString);
		System.out.println("------------------------------------");
		System.out.println("Try get.");
		System.out.println("Return count: " + entityList.getCount());
		for( Object element: entityList.toArray() ) {
			Thing ob = (Thing) element;
			System.out.println(ob.getId() + " , " + ob.getName() + " , " + ob.getDescription());
		}
		
		
		//Try Update
		queryString = "$filter=name eq \'myThing_2\' and description eq \'Description for myThing_2\'";
		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.THING, queryString);
		Thing target = (Thing) entityList.toArray()[0];
		target.setName("mything2_Updated");
		target.setDescription("Updated description for myThing_2");
		IdString id = (IdString) target.getId();
		success = mmsk.update(target, id);
		mmsk.ppm.commit();
		System.out.println("------------------------------------");
		System.out.println("Try Update myThing_2.");
		
		// Output result using Get
		queryString = "$count=true";
		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.THING, queryString);
		System.out.println("------------------------------------");
		System.out.println("Try get.");
		System.out.println("Return count: " + entityList.getCount());
		for( Object element: entityList.toArray() ) {
			Thing ob = (Thing) element;
			System.out.println(ob.getId() + " , " + ob.getName() + " , " + ob.getDescription());
		}


		//Try Delete
		queryString = "$filter=name eq \'myThing_3\' and description eq \'Description for myThing_3\'";
		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.THING, queryString);
		target = (Thing) entityList.toArray()[0];
		id = (IdString) target.getId();
		success = mmsk.delete(EntityType.THING, id);
		mmsk.ppm.commit();
		System.out.println("------------------------------------");
		System.out.println("Try Delete myThing_3.");
		
		// Output result using Get
		queryString = "$count=true";
		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.THING, queryString);
		System.out.println("------------------------------------");
		System.out.println("Try get.");
		System.out.println("Return count: " + entityList.getCount());
		for( Object element: entityList.toArray() ) {
			Thing ob = (Thing) element;
			System.out.println(ob.getId() + " , " + ob.getName() + " , " + ob.getDescription());
		}
		
		
//		//Try Insert deeper entities
//		ObservedProperty op1 = new ObservedProperty();
//		op1.setName("Temperature");
//		op1.setDescription("ObservedProperty used for testing ds1");
//		op1.setDefinition("Float");
//		success = mmsk.insert(op1);
//		
//		Sensor ss1 = new Sensor();
//		ss1.setName("Temperature Sensor_1");
//		ss1.setDescription("Sensor used for testing ds1");
//		ss1.setEncodingType("utf8");
//		ss1.setMetadata("true");
//		success = mmsk.insert(ss1);
//		
//		Datastream ds1 = new Datastream();
//		ds1.setName("Datastream1");
//		ds1.setDescription("Datastream1, owned by myThing_1");
//		HashMap<String, Object> h = new HashMap<String, Object>(){{put("pp",100);}};
//		ds1.setProperties(h);
//		ds1.setObservationType("filetype");
//		ds1.setObservedArea(new Point(1.0,1.0,1.0,1.0));
//		ds1.setPhenomenonTime(TimeInterval.create(100l, 110l));
//		ds1.setResultTime(TimeInterval.create(100l, 110l));
//		ds1.setSensor(ss1);
//		ds1.setThing(myThing1);
//		ds1.setUnitOfMeasurement(new UnitOfMeasurement("Degree", "C", "degree C"));
//		ds1.setObservedProperty(op1);
//		success = mmsk.insert(ds1);
//		
//		ObservedProperty op2 = new ObservedProperty();
//		op2.setName("Height");
//		op2.setDescription("ObservedProperty used for testing ds2");
//		op2.setDefinition("Float");
//		success = mmsk.insert(op2);
//		
//		Sensor ss2 = new Sensor();
//		ss2.setName("Height Sensor_1");
//		ss2.setDescription("Sensor used for testing ds2");
//		ss2.setEncodingType("utf8");
//		ss2.setMetadata("true");
//		success = mmsk.insert(ss2);
//		
//		Datastream ds2 = new Datastream();
//		ds2.setName("Datastream2");
//		ds2.setDescription("Datastream2, owned by myThing_1");
//		ds2.setProperties(h);
//		ds2.setObservationType("filetype");
//		ds2.setObservedArea(new Point(1.0,1.0,1.0,1.0));
//		ds2.setPhenomenonTime(TimeInterval.create(100l, 110l));
//		ds2.setResultTime(TimeInterval.create(100l, 110l));
//		ds2.setSensor(ss2);
//		ds2.setThing(myThing1);
//		ds2.setUnitOfMeasurement(new UnitOfMeasurement("Meter", "m", "distance"));
//		ds2.setObservedProperty(op2);
//		success = mmsk.insert(ds2);
//		
//		FeatureOfInterest foi = new FeatureOfInterest();
//		foi.setEncodingType("Point");
//		foi.setFeature(new Point(1.0,1.0,1.0,1.0));
//		success = mmsk.insert(foi);
//		
//		Observation os1 = new Observation();
//		TimeInstant t = TimeInstant.now();
//		os1.setPhenomenonTime(t);
//		os1.setResultTime(t);
//		os1.setResult(100);
//		os1.setDatastream(ds1);
//		os1.setFeatureOfInterest(foi);
//		success = mmsk.insert(os1);
//		
//		Observation os2 = new Observation();
//		t = TimeInstant.now();
//		os2.setPhenomenonTime(t);
//		os2.setResultTime(t);
//		os2.setResult(105);
//		os2.setDatastream(ds2);
//		os2.setFeatureOfInterest(foi);
//		success = mmsk.insert(os2);
//		
//		Observation os3 = new Observation();
//		t = TimeInstant.now();
//		os3.setPhenomenonTime(t);
//		os3.setResultTime(t);
//		os3.setResult(110);
//		os3.setDatastream(ds2);
//		os3.setFeatureOfInterest(foi);
//		success = mmsk.insert(os3);
//		
//		System.out.println("------------------------------------");
//		System.out.println("Try deeper insert.");
//		mmsk.ppm.commit();
//		
//		
//		// Output result using Get
//		queryString = "$count=true";
//		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.OBSERVATION, queryString);
//		System.out.println("------------------------------------");
//		System.out.println("Try get.");
//		System.out.println("Return count: " + entityList.getCount());
//		for( Object element: entityList.toArray() ) {
//			Observation ob = (Observation) element;
//			System.out.println(ob.getId() + " , " + ob.getResultTime() + " , " + ob.getResult());
//		}
//		
//		
//		//Try Update deeper entities
//		queryString = "$filter=result eq 105";
//		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.OBSERVATION, queryString);
//		Observation targetob = (Observation) entityList.toArray()[0];
//		
//		targetob.setResult(500);
//		mmsk.update(targetob, targetob.getId());
//		System.out.println("------------------------------------");
//		System.out.println("Try deeper update.");
//		mmsk.ppm.commit();
//		
//		
//		// Output result using Get
//		queryString = "$count=true";
//		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.OBSERVATION, queryString);
//		System.out.println("------------------------------------");
//		System.out.println("Try get.");
//		System.out.println("Return count: " + entityList.getCount());
//		for( Object element: entityList.toArray() ) {
//			Observation ob = (Observation) element;
//			System.out.println(ob.getId() + " , " + ob.getResultTime() + " , " + ob.getResult());
//		}
//		
//		
//		//Try Delete deeper entities
//		queryString = "$filter=result eq 110";
//		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.OBSERVATION, queryString);
//		targetob = (Observation) entityList.toArray()[0];
//
//		mmsk.delete(EntityType.OBSERVATION, targetob.getId());
//		System.out.println("------------------------------------");
//		System.out.println("Try deeper delete.");
//		mmsk.ppm.commit();
//		
//		
//		// Output result using Get
//		queryString = "$count=true";
//		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.OBSERVATION, queryString);
//		System.out.println("------------------------------------");
//		System.out.println("Try get.");
//		System.out.println("Return count: " + entityList.getCount());
//		for( Object element: entityList.toArray() ) {
//			Observation ob = (Observation) element;
//			System.out.println(ob.getId() + " , " + ob.getResultTime() + " , " + ob.getResult());
//		}
//		
//		
//		//Try Delete middle entities
//		queryString = "$filter=name eq\'Datastream1\'";
//		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.DATASTREAM, queryString);
//		Datastream targetds = (Datastream) entityList.toArray()[0];
//
//		mmsk.delete(EntityType.DATASTREAM, targetds.getId());
//		System.out.println("------------------------------------");
//		System.out.println("Try middle delete.");
//		mmsk.ppm.commit();
//		
//		// Output result using Get
//		queryString = "$count=true";
//		entityList = (EntitySetImpl) mmsk.getEntityByQuery(EntityType.OBSERVATION, queryString);
//		System.out.println("------------------------------------");
//		System.out.println("Try get.");
//		System.out.println("Return count: " + entityList.getCount());
//		for( Object element: entityList.toArray() ) {
//			Observation ob = (Observation) element;
//			System.out.println(ob.getId() + " , " + ob.getResultTime() + " , " + ob.getResult());
//		}
//		
//		
		mmsk.ppm.close();
	}
}
