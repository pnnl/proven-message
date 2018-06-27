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

import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

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
	
	private String name;
	private String label;
	private String value;
	private boolean isMetadata;
	private MetricValueType valueType;

	public ProvenMetric() {
	}

	public ProvenMetric(String name, String label, String value, boolean isMetadata, MetricValueType valueType) {
		this.name = name;
		this.label = label;
		this.value = value;
		this.isMetadata = isMetadata;
		this.valueType = valueType;
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {

		this.name = in.readUTF();
		this.label = in.readUTF();
		this.value = in.readUTF();
		this.isMetadata = in.readBoolean();
		this.valueType = MetricValueType.valueOf(in.readUTF());
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {

		out.writeUTF(this.name);
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

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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

	public MetricValueType getValueType() {
		return valueType;
	}

	public void setValueType(MetricValueType valueType) {
		this.valueType = valueType;
	}

	/**
	 * Identifies possible types for a metric value. Based on XSD typing, these
	 * can be simple types (e.g. string, integer, boolean, etc.) or types
	 * specific to values originating from the API (e.g. host_name, process_id,
	 * etc.).
	 * 
	 * @author d3j766
	 * 
	 */
	public static enum MetricValueType {

		STRING("xsd:string"),

		INTEGER("xsd:integer"),

		LONG("xsd:long"),

		BOOLEAN("xsd:boolean"),

		DATE_TIME("xsd:dateTime"),

		TIMESTAMP("xsd:long"),

		HOST_NAME("xsd:string"),

		HOST_FQDN("xsd:string"),

		APPLICATION_NAME("xsd:string"),

		APPLICATION_VERSION("xsd:string"),

		PROCESS_ID("xsd:integer"),

		FLOAT("xsd:float"),

		DOUBLE("xsd:double");

		private String xsdType;

		MetricValueType(String xsdType) {
			this.xsdType = xsdType;
		}

		public String getXsdType() {
			return this.xsdType;
		}
	}

}
