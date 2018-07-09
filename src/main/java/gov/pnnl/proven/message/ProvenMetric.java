/*******************************************************************************
 * Copyright (c) 2017, Battelle Memorial Institute All rights reserved.
 * Battelle Memorial Institute (hereinafter Battelle) hereby grants permission to any person or entity 
 * lawfully obtaining a copy of this software and associated documentation files (hereinafter the 
 * Software) to redistribute and use the Software in source and binary forms, with or without modification. 
 * Such person or entity may use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of 
 * the Software, and may permit others to do so, subject to the following conditions:
 * Redistributions of source code must retain the above copyright notice, this list of conditions and the 
 * following disclaimers.
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and 
 * the following disclaimer in the documentation and/or other materials provided with the distribution.
 * Other than as used herein, neither the name Battelle Memorial Institute or Battelle may be used in any 
 * form whatsoever without the express written consent of Battelle.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY 
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL 
 * BATTELLE OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, 
 * OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE 
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED 
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED 
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * General disclaimer for use with OSS licenses
 * 
 * This material was prepared as an account of work sponsored by an agency of the United States Government. 
 * Neither the United States Government nor the United States Department of Energy, nor Battelle, nor any 
 * of their employees, nor any jurisdiction or organization that has cooperated in the development of these 
 * materials, makes any warranty, express or implied, or assumes any legal liability or responsibility for 
 * the accuracy, completeness, or usefulness or any information, apparatus, product, software, or process 
 * disclosed, or represents that its use would not infringe privately owned rights.
 * 
 * Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer, 
 * or otherwise does not necessarily constitute or imply its endorsement, recommendation, or favoring by the United 
 * States Government or any agency thereof, or Battelle Memorial Institute. The views and opinions of authors expressed 
 * herein do not necessarily state or reflect those of the United States Government or any agency thereof.
 * 
 * PACIFIC NORTHWEST NATIONAL LABORATORY operated by BATTELLE for the 
 * UNITED STATES DEPARTMENT OF ENERGY under Contract DE-AC05-76RL01830
 ******************************************************************************/

package gov.pnnl.proven.message;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import gov.pnnl.proven.message.MessageContent.MessageContentStream;

import static gov.pnnl.proven.message.MessageUtils.*;

/**
 * Proven's general representation of a single time-series metric value. A
 * {@link ProvenMeasurement} is composed of one or more metrics.
 * 
 * @author d3j766
 *
 */
@XmlRootElement
public class ProvenMetric implements IdentifiedDataSerializable, Serializable {

	private static final long serialVersionUID = 1L;

	private static Logger log = LoggerFactory.getLogger(ProvenMetric.class);

	private String label;
	private String value;
	private boolean isMetadata;
	private MetricFragmentIdentifier.MetricValueType valueType;

	public ProvenMetric() {
	}

	public ProvenMetric(String label, String value, boolean isMetadata,
			MetricFragmentIdentifier.MetricValueType valueType) {
		this.label = label;
		this.value = value;
		this.isMetadata = isMetadata;
		this.valueType = valueType;
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {

		this.label = in.readUTF();
		this.value = in.readUTF();
		this.isMetadata = in.readBoolean();
		this.valueType = MetricFragmentIdentifier.MetricValueType.valueOf(in.readUTF());
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {

		out.writeUTF(this.label);
		out.writeUTF(this.value);
		out.writeBoolean(this.isMetadata);
		out.writeUTF(this.valueType.toString());
	}

	@Override
	public int getFactoryId() {
		return ProvenMessageIDSFactory.FACTORY_ID;
	}

	@Override
	public int getId() {
		return ProvenMessageIDSFactory.PROVEN_METRIC_TYPE;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public boolean isMetadata() {
		return isMetadata;
	}

	public void setMetadata(boolean isMetadata) {
		this.isMetadata = isMetadata;
	}

	public MetricFragmentIdentifier.MetricValueType getValueType() {
		return valueType;
	}

	public void setValueType(MetricFragmentIdentifier.MetricValueType valueType) {
		this.valueType = valueType;
	}

	/**
	 * A {@link ProvenMetric} builder, uses URI fragment identifier of the
	 * metric's Value Type. Proven metric data comes from a
	 * {@link ProvenMessage} {@link MessageContentStream} originally as semantic
	 * data. URI fragment identifiers for the Value Type are used to provide
	 * metric information to facilitate creation and storage into the registered
	 * time-series store, if any.
	 * 
	 * TODO ALlow for external definition metric fragment identifiers
	 * 
	 * @author d3j766
	 *
	 */
	public static class MetricFragmentIdentifier {

		public static final String TS_TAG = "TimeSeriesTag";
		public static final String PROVEN_TS_TAG_RES = PROVEN_MESSAGE_NS + TS_TAG;
		public static final String TS_FIELD = "TimeSeriesField";
		public static final String PROVEN_TS_FIELD_RES = PROVEN_MESSAGE_NS + TS_FIELD;
		public static final String MFI_SPLIT_DELIMETER = ":";

		public enum MFIParam {
			MetricType, Label, ValueType;
		}

		public enum MetricType {

			Field(PROVEN_TS_FIELD_RES), Tag(PROVEN_TS_TAG_RES);

			String resName;

			MetricType(String resName) {
				this.resName = resName;
			}

			public String getResName() {
				return resName;
			}

		}

		public enum MetricValueType {
			Integer, Long, Float, Double, Boolean, String, Derive;
		}

		public static boolean isProvenMetric(Triple t3) {

			boolean ret = false;
			if (null != t3) {
				Node tObject = t3.getObject();
				if (tObject.isLiteral()) {
					String literalDT = tObject.getLiteralDatatypeURI();
					if (null != literalDT) {
						if ((literalDT.startsWith(MetricType.Field.getResName()))
								|| (literalDT.startsWith(MetricType.Tag.getResName()))) {
							ret = true;
						}
					}
				}
			}

			return ret;
		}

		private static MetricValueType deriveValueType(String value) {

			MetricValueType ret = null;

			try {

				Integer intValue = Integer.valueOf(value);
				return MetricValueType.Integer;

			} catch (NumberFormatException e) {
			}

			try {

				Long Value = Long.valueOf(value);
				return MetricValueType.Long;

			} catch (NumberFormatException e) {
			}

			try {

				Float Value = Float.valueOf(value);
				return MetricValueType.Float;

			} catch (NumberFormatException e) {
			}

			try {

				Double Value = Double.valueOf(value);
				return MetricValueType.Double;

			} catch (NumberFormatException e) {
			}

			try {

				Boolean Value = Boolean.valueOf(value);
				return MetricValueType.Boolean;

			} catch (NumberFormatException e) {
			}

			// If here, then default to String type
			return MetricValueType.String;
		}

		public static ProvenMetric buildProvenMetric(Triple t3) {

			ProvenMetric ret = null;

			if (isProvenMetric(t3)) {

				// Nodes and valueType string
				Node tSubject = t3.getSubject();
				Node tPredicate = t3.getPredicate();
				Node tObject = t3.getObject();
				String valueTypeStr = tObject.getLiteralDatatypeURI();
				String value = tObject.getLiteral().getLexicalForm();

				// Default values
				String defLabel = tPredicate.getLocalName();
				boolean defIsMetadata = true;
				MetricValueType defValueType = MetricValueType.String;

				// Fragment values
				String fragLabel = null;
				Boolean fragIsMetadata = null;
				MetricValueType fragValueType = null;

				// Get values from Fragment, if any
				URI typeURI;
				try {
					typeURI = new URI(valueTypeStr);
				} catch (URISyntaxException e) {
					typeURI = null;
				}
				if (null != typeURI) {
					String MFIStr = typeURI.getFragment();
					String[] params = null;
					if (null != MFIStr) {
						int maxValues = MFIParam.values().length;
						params = MFIStr.split(MFI_SPLIT_DELIMETER, maxValues);
					}
					if (null != params) {

						for (int i = 0; i <= params.length - 1; i++) {

							switch (MFIParam.values()[i]) {

							case MetricType:

								// Default is TAG meta-data. Will not be an empty string.
								if (params[i].equals(TS_FIELD)) {
									fragIsMetadata = false;
								}
								break;

							case Label:
								fragLabel = (params[i].isEmpty()) ? null : params[i];
								break;

							case ValueType:
								if (!(params[i].isEmpty())) {
									fragValueType = MetricValueType.valueOf(params[i]);
									if (fragValueType == MetricValueType.Derive) {
										fragValueType = deriveValueType(value);
									}
								}
								break;
							default:
								break;
							}
						}
					}

				}

				// Create new proven metric
				ret = new ProvenMetric(((null == fragLabel) ? defLabel : fragLabel), value,
						((null == fragIsMetadata) ? defIsMetadata : fragIsMetadata),
						((null == fragValueType) ? defValueType : fragValueType));

			}

			return ret;
		}

	}
}
