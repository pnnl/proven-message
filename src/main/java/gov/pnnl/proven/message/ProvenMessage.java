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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import gov.pnnl.proven.message.exception.InvalidProvenMessageException;

/**
 * General messaging construct used by Proven to communicate data between Proven
 * application components. Proven messages have global identification and a
 * defined processing life-cycle within a Proven cluster member.
 * 
 * @author raju332
 * @author d3j766
 *
 */
@XmlRootElement
public class ProvenMessage implements IdentifiedDataSerializable, Serializable {

	private static final long serialVersionUID = 1L;

	private static Logger log = LoggerFactory.getLogger(ProvenMessage.class);

	/**
	 * Message KEY
	 * 
	 * Message keys are used to identify proven messages in the memory grid of
	 * Proven's hybrid store.
	 */
	public static final String MESSAGE_KEY_DELIMETER = "^||^";

	public enum MessageKeyPartOrder {
		MessageId, Domain, Name, Source, Created;
	};

	public String getMessageKey() {

		String ret = "";
		ret += this.messageId + MESSAGE_KEY_DELIMETER;
		ret += ((this.domain == null) ? "" : this.domain) + MESSAGE_KEY_DELIMETER;
		ret += ((this.name == null) ? "" : this.name) + MESSAGE_KEY_DELIMETER;
		ret += ((this.source == null) ? "" : this.source) + MESSAGE_KEY_DELIMETER;
		ret += this.messageProperties.getCreated();

		return ret;
	}

	/**
	 * Messages are assigned an identifier, making it unique across disclosure
	 * sources.
	 */
	@XmlJavaTypeAdapter(UUIDAdapter.class)
	private UUID messageId;

	/**
	 * The provided unprocessed message content, used to construct a new
	 * ProvenMessage.
	 */
	private String message;

	/**
	 * Identifies {@link MessageContent}
	 */
	private MessageContent messageContent;

	/**
	 * Message name is assigned at construction, if not provided. The name
	 * cannot be modified once assigned.
	 */
	private String name;

	/**
	 * Identifies messages domain, all messages must be associated with a
	 * domain. If one is not provided, Proven provides a default domain for
	 * which the message will be associated. In Proven, a domain represents a
	 * discrete sphere or model of activity or knowledge. That is, it identifies
	 * a grouping of knowledge that is managed separately from other domain
	 * knowledge models. In proven's hybrid store, domain knowledge is isolated
	 * by sub-graphs in its semantic store, by databases in time-series store,
	 * and by key values in distributed Map structures in the memory grid.
	 */
	private String domain;

	/**
	 * If true, message content will not be persisted in hybrid store. Default
	 * is false.
	 */
	private boolean isTransient;

	/**
	 * If true, message content will remain in memory grid facet of hybrid store
	 * unless explicitly removed, @see {@link MessageContent#Static}. Default is
	 * false.
	 */
	private boolean isStatic;

	/**
	 * Identifies source of the message (e.g. proven-client).
	 */
	private String source;

	/**
	 * Listing of keywords that can be associated with the message for query and
	 * discovery purposes.
	 */
	private Collection<String> keywords;

	/**
	 * Properties associated with message.
	 * 
	 * TODO Make serializable and non-transient
	 */
	private MessageProperties messageProperties = new MessageProperties();

	/**
	 * Collection of measurements include in message, if any.
	 */
	private Collection<ProvenMeasurement> measurements;

	/**
	 * Collection of RDF Statements included in message, if any.
	 */
	private Collection<ProvenStatement> statements;

	/**
	 * Identifies query objects. Only applicable for
	 * {@link MessageContent#Query}
	 */
	private ProvenQueryTimeSeries tsQuery;

	/**
	 * Use static method {@link ProvenMessage#message(String)} to obtain a
	 * {@link ProvenMessage.ProvenMessageBuilder} instance and obtain a
	 * ProvenMessage using the builder. ProvenMessage requires at a minimum a
	 * JSON message; other parameters will be assigned values if missing. The
	 * provided JSON is transformed into JSON-LD for storage into Proven's
	 * hybrid store. Message content may be provided in JSON-LD form, making
	 * JSON transform unnecessary.
	 */
	public ProvenMessage() {
	}

	/**
	 * Creates a new {@code ProvenMessageBuilder} by providing the JSON message
	 * content.
	 * 
	 * @param message
	 *            a JSON string, basis for new {@code ProvenMessage}.
	 * @return a new {@code ProvenMessageBuilder}
	 * 
	 */
	public static ProvenMessageBuilder message(String message) {
		ProvenMessageBuilder pmb = ProvenMessageBuilder.getInstance(message);
		return pmb;
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {

		this.messageId = UUID.fromString(in.readUTF());
		this.message = in.readUTF();
		this.messageContent = MessageContent.valueOf(in.readUTF());
		this.name = in.readUTF();
		this.domain = in.readUTF();
		this.isTransient = in.readBoolean();
		this.isStatic = in.readBoolean();
		this.source = in.readUTF();
		this.keywords = in.readObject();
		this.messageProperties = in.readObject();
		this.measurements = in.readObject();
		this.statements = in.readObject();
		this.tsQuery = in.readObject();
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {

		out.writeUTF(this.messageId.toString());
		out.writeUTF(this.message);
		out.writeUTF(this.messageContent.toString());
		out.writeUTF(this.name);
		out.writeUTF(this.domain);
		out.writeBoolean(this.isTransient);
		out.writeBoolean(this.isStatic);
		out.writeUTF(this.source);
		out.writeObject(this.keywords);
		out.writeObject(this.messageProperties);
		out.writeObject(this.measurements);
		out.writeObject(this.statements);
		out.writeObject(this.tsQuery);
	}

	@Override
	public int getFactoryId() {
		return ProvenMessageIDSFactory.FACTORY_ID;
	}

	@Override
	public int getId() {
		return ProvenMessageIDSFactory.PROVEN_MESSAGE_TYPE;
	}

	public UUID getMessageId() {
		return messageId;
	}

	public String getName() {
		return name;
	}

	public String getDomain() {
		return domain;
	}

	public MessageContent getMessageContent() {
		return messageContent;
	}

	public boolean isTransient() {
		return isTransient;
	}

	public boolean isStatic() {
		return isStatic;
	}

	public String getSource() {
		return source;
	}

	public Collection<String> getKeywords() {
		return keywords;
	}

	public MessageProperties getMessageProperties() {
		return messageProperties;
	}

	public String getMessage() {
		return message;
	}

	public Collection<ProvenMeasurement> getMeasurements() {
		return measurements;
	}

	public Collection<ProvenStatement> getStatements() {
		return statements;
	}

	public ProvenQueryTimeSeries getTsQuery() {
		return tsQuery;
	}

	public void setMessageId(UUID messageId) {
		this.messageId = messageId;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setMessageContent(MessageContent messageContent) {
		this.messageContent = messageContent;
	}

	public void setTransient(boolean isTransient) {
		this.isTransient = isTransient;
	}

	public void setStatic(boolean isStatic) {
		this.isStatic = isStatic;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}

	public void setMessageProperties(MessageProperties messageProperties) {
		this.messageProperties = messageProperties;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void setMeasurements(List<ProvenMeasurement> measurements) {
		this.measurements = measurements;
	}

	public void setStatements(Collection<ProvenStatement> statements) {
		this.statements = statements;
	}

	public void setTsQuery(ProvenQueryTimeSeries tsQuery) {
		this.tsQuery = tsQuery;
	}

	/**
	 * A class used to build ProvenMessage instances. The JSON message string,
	 * at a minimum, must be provided to successfully build a
	 * {@code ProvenMessage}.
	 * 
	 * @author d3j766
	 *
	 */
	@XmlTransient
	public static class ProvenMessageBuilder {

		private static Logger log = LoggerFactory.getLogger(ProvenMessageBuilder.class);

		private UUID messageId = UUID.randomUUID();
		private String message;
		private MessageContent messageContent = MessageContent.Explicit;
		private String name;
		private String domain;
		private boolean isTransient = false;
		private boolean isStatic = false;
		private String source;
		private List<String> keywords = new ArrayList<String>();
		private String messageKey;

		/**
		 * Constructor is protected, use static method
		 * {@code ProvenMessage#message(String)} to get the builder instance.
		 * 
		 * @param message
		 *            the message content
		 */
		protected ProvenMessageBuilder(String message) {
			this.message = message;
		}

		/**
		 * Creates and returns a new builder instance.
		 * 
		 * @param message
		 *            the message content
		 * @return the {@code ProvenMessageBuilder}
		 */
		protected static ProvenMessageBuilder getInstance(String message) {
			return new ProvenMessageBuilder(message);
		}

		/**
		 * Adds a message to builder, if one exists it will be replaced.
		 * 
		 * @param message
		 *            the message content
		 * 
		 * @return the {@code ProvenMessageBuilder}
		 * 
		 */
		public ProvenMessageBuilder message(String message) {
			this.message = message;
			return this;
		}

		/**
		 * Adds a message name. If no name is available at build, the builder
		 * will generate a new message name.
		 * 
		 * @param name
		 *            the message name
		 * 
		 * @return the {@code ProvenMessageBuilder}
		 * 
		 */
		public ProvenMessageBuilder name(String name) {
			this.name = name;
			return this;
		}

		/**
		 * Associates message with a domain. If no domain is provided, message
		 * will be associated with Proven's "default" domain.
		 * 
		 * @param domain
		 *            the message's domain
		 * 
		 * @return the {@code ProvenMessageBuilder}
		 * 
		 * 
		 */
		public ProvenMessageBuilder domain(String domain) {
			this.domain = domain;
			return this;
		}

		/**
		 * Describes source of message. Default is no source description.
		 * 
		 * @param source
		 *            the message's source description
		 * 
		 * @return the {@code ProvenMessageBuilder}
		 * 
		 */
		public ProvenMessageBuilder source(String source) {
			this.source = source;
			return this;
		}

		/**
		 * Indicates if the message is transient. Default is false.
		 * 
		 * @param isTransient
		 *            if true, indicates message will not be persisted to hybrid
		 *            store.
		 * 
		 * @return the {@code ProvenMessageBuilder}
		 */
		public ProvenMessageBuilder isTransient(boolean isTransient) {
			this.isTransient = isTransient;
			return this;
		}

		/**
		 * Indicates if message is static. Default is false.
		 * 
		 * @param isStatic
		 *            if true, indicates message is
		 *            {@link MessageContent#Static}
		 * 
		 * @return the {@code ProvenMessageBuilder}
		 *
		 */
		public ProvenMessageBuilder isStatic(boolean isStatic) {
			this.isStatic = isStatic;
			return this;
		}

		/**
		 * Provides keywords to associate with message. Default is no keywords.
		 * 
		 * @param keywords
		 *            a list of keywords keywords are associated with message.
		 *            Default is no keywords.
		 * 
		 * @return the {@code ProvenMessageBuilder}
		 * 
		 */
		public ProvenMessageBuilder keywords(List<String> keywords) {
			this.keywords = keywords;
			return this;
		}

		/**
		 * Identifies message content type, default is
		 * {@code MessageContent#Explicit}.
		 * 
		 * @param messageContent
		 *            the message's content type. Default is
		 *            {@link MessageContent#Explicit}
		 * 
		 * @return the {@code ProvenMessageBuilder}
		 * 
		 */
		public ProvenMessageBuilder messageContent(MessageContent messageContent) {
			this.messageContent = messageContent;
			return this;
		}

		/**
		 * Builds and returns a new ProvenMessage using current builder
		 * settings.
		 * 
		 * @return a {@code ProvenMessage}
		 * @throws InvalidProvenMessageException
		 *             if build fails a {@link InvalidProvenMessageException} is
		 *             thrown.
		 */
		public ProvenMessage build() throws InvalidProvenMessageException {

			// Create a new proven message and transfer data from builder
			ProvenMessage pm = new ProvenMessage();
			pm.messageId = this.messageId;
			pm.message = this.message;
			pm.messageContent = this.messageContent;
			pm.name = this.name;
			pm.domain = this.domain;
			pm.isTransient = this.isTransient;
			pm.isStatic = this.isStatic;
			pm.source = this.source;
			pm.keywords = this.keywords;

			try {

				// Construct initial data model
				String message = MessageUtils.prependContext(pm.message);
				Model dataModel = MessageUtils.createMessageDataModel(pm, message);

				// TODO determine how/if should utilize OWL reasoning
				// dataModel = MessageUtils.addHierarchies(dataModel);

				// SHACL rule processing to produce final message data model
				dataModel = MessageUtils.addShaclRuleResults(dataModel);

				// Save model statements in proven message
				pm.statements = MessageUtils.getProvenStatements(dataModel);

				// Set measurements if explicit content
				if (pm.messageContent == MessageContent.Explicit) {
					pm.measurements = MessageUtils.getProvenMeasurements(dataModel);
				}

				// Set query if query content
				if (pm.messageContent == MessageContent.Query) {
					pm.tsQuery = MessageUtils.getProvenQuery(dataModel);
				}

			} catch (Exception e) {
				throw new InvalidProvenMessageException("Failed to build message", e);
			}

			return pm;
		}

	}
}
