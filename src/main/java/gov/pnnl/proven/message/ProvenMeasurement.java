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
import java.util.HashSet;
import java.util.Set;
import javax.xml.bind.annotation.XmlRootElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * Proven's general representation of a single time-series measurement data
 * point.
 * 
 * @author d3j766
 *
 */
@XmlRootElement
public class ProvenMeasurement implements IdentifiedDataSerializable, Serializable {

	private static final long serialVersionUID = 1L;

	private static Logger log = LoggerFactory.getLogger(ProvenMeasurement.class);

	/**
	 * Name of measurement, identifies a time-series measurement container. If
	 * null, proven's storage component is responsible for measurement
	 * assignment. Provides a default.
	 */
	private String measurementName = MessageUtils.DEFAULT_MEASUREMENT;

	/**
	 * Date/Time of measurement. If null, proven's storage component responsible
	 * for timestamp assignment.
	 */
	private Long timestamp;

	/**
	 * Semantic link to proven message concept instance.
	 */
	URI provenMessage;

	/**
	 * Semantic link to proven message's measurement concept instance.
	 */
	URI provenMessageMeasurement;

	/**
	 * The set of measurement values and meta-data contained in the measurement.
	 */
	private Set<ProvenMetric> metrics = new HashSet<ProvenMetric>();

	public ProvenMeasurement() {
	}

	/**
	 * Creates a new measurement.
	 * 
	 * @param name
	 *            measurement name
	 * @param timestamp
	 *            measurement timestamp
	 * @param metrics
	 *            set of metric values
	 */
	public ProvenMeasurement(String name, Long timestamp, Set<ProvenMetric> metrics) {
		this.measurementName = name;
		this.timestamp = timestamp;
		this.metrics = metrics;
	}

	public void addMetric(ProvenMetric metric) {
		// TODO Capture statistics here
		metrics.add(metric);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {

		this.measurementName = in.readUTF();
		String timestampStr = in.readUTF();
		this.timestamp = ((timestampStr.isEmpty()) ? null : Long.valueOf(timestampStr));
		String provenMessageStr = in.readUTF();
		this.provenMessage = ((provenMessageStr.isEmpty()) ? null : URI.create(provenMessageStr));
		String provenMessageMeasurementStr = in.readUTF();
		this.provenMessageMeasurement = ((provenMessageMeasurementStr.isEmpty()) ? null
				: URI.create(provenMessageMeasurementStr));
		int count = in.readInt();
		for (int i = 0; i < count; i++) {
			ProvenMetric metric = new ProvenMetric();
			metric.readData(in);
			metrics.add(metric);
		}
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {

		out.writeUTF(this.measurementName);
		out.writeUTF((null == this.timestamp) ? ("") : this.timestamp.toString());
		out.writeUTF((null == this.provenMessage) ? ("") : this.provenMessage.toString());
		out.writeUTF((null == this.provenMessageMeasurement) ? ("") : this.provenMessageMeasurement.toString());
		out.writeInt(((null == metrics) ? 0 : metrics.size()));
		for (ProvenMetric metric : metrics) {
			metric.writeData(out);
		}
	}

	@Override
	public int getFactoryId() {
		return ProvenMessageIDSFactory.FACTORY_ID;
	}

	@Override
	public int getId() {
		return ProvenMessageIDSFactory.PROVEN_MEASUREMENT_TYPE;
	}

	public String getMeasurementName() {
		return measurementName;
	}

	public void setMeasurementName(String measurementName) {
		this.measurementName = measurementName;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public URI getProvenMessage() {
		return provenMessage;
	}

	public URI getProvenMessageMeasurement() {
		return provenMessageMeasurement;
	}

	public void setProvenMessage(URI provenMessage) {
		this.provenMessage = provenMessage;
	}

	public void setProvenMessageMeasurement(URI provenMessageMeasurement) {
		this.provenMessageMeasurement = provenMessageMeasurement;
	}

	public Set<ProvenMetric> getMetrics() {
		return metrics;
	}

	public void setMetrics(Set<ProvenMetric> metrics) {
		this.metrics = metrics;
	}

}
