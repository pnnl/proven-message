package gov.pnnl.proven.message;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * 
 * IdentifiedDataSerializable factory for Proven Messages
 * 
 * @author d3j766
 *
 */
public class ProvenMessageIDSFactory implements DataSerializableFactory {

	public ProvenMessageIDSFactory() {
	}
	
	// Factory
	public static final int FACTORY_ID = 1;

	// Serializable types
	public static final int MESSAGE_PROPERTIES_TYPE = 1;
	public static final int PROVEN_MEASUREMENT_TYPE = 2;
	public static final int PROVEN_MESSAGE_TYPE = 3;
	public static final int PROVEN_MESSAGE_RESPONSE_TYPE = 4;
	public static final int PROVEN_METRIC_TYPE = 5;
	public static final int PROVEN_QUERY_FILTER_TYPE = 6;
	public static final int PROVEN_QUERY_TIME_SERIES_TYPE = 7;
	public static final int PROVEN_STATEMENT_TYPE = 8;

	@Override
	public IdentifiedDataSerializable create(int typeId) {

		switch (typeId) {

		case (MESSAGE_PROPERTIES_TYPE):
			return new MessageProperties();
		case (PROVEN_MEASUREMENT_TYPE):
			return new ProvenMeasurement();
		case (PROVEN_MESSAGE_TYPE):
			return new ProvenMessage();
		case (PROVEN_MESSAGE_RESPONSE_TYPE):
			return new ProvenMessageResponse();
		case (PROVEN_METRIC_TYPE):
			return new ProvenMetric();
		case (PROVEN_QUERY_FILTER_TYPE):
			return new ProvenQueryFilter();
		case (PROVEN_QUERY_TIME_SERIES_TYPE):
			return new ProvenQueryTimeSeries();
		case (PROVEN_STATEMENT_TYPE):
			return new ProvenStatement();
		default:
			return null;
		}
	}

}
