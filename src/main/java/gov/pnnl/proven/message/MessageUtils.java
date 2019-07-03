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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.jena.graph.BlankNodeId;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.topbraid.shacl.rules.RuleUtil;
import gov.pnnl.proven.message.exception.InvalidProvenMeasurementException;
import gov.pnnl.proven.message.exception.InvalidProvenMessageException;
import gov.pnnl.proven.message.exception.InvalidProvenQueryException;
import gov.pnnl.proven.message.exception.InvalidProvenStatementsException;

/**
 * Provides utility methods supporting the construction and validation of Proven
 * messages.
 * 
 * @author d3j766
 *
 */
public class MessageUtils {

	private static Logger log = LoggerFactory.getLogger(MessageUtils.class);

	public static final int NULL_LIST = -1;
	public static final String PROVEN_MESSAGE_NS = "http://proven.pnnl.gov/proven-message#";
	public static final String PROVEN_MESSAGE_RES = PROVEN_MESSAGE_NS + "ProvenMessage";
	public static final String PROVEN_MEASUREMENT_RES = PROVEN_MESSAGE_NS + "Measurement";
	public static final String PROVEN_QUERY_FILTER_RES = PROVEN_MESSAGE_NS + "QueryFilter";
	public static final String RDF_TYPE_PROP = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	public static final String QUERY_TYPE_PROP = PROVEN_MESSAGE_NS + "hasQueryType";
	public static final String MESSAGE_CONTENT_PROP = PROVEN_MESSAGE_NS + "hasMessageContent";
	public static final String NAME_PROP = PROVEN_MESSAGE_NS + "hasName";
	public static final String TIMESTAMP_PROP = PROVEN_MESSAGE_NS + "hasTimestamp";
	public static final String QUERY_MEASUREMENT_PROP = PROVEN_MESSAGE_NS +  "hasQueryMeasurement";
	public static final String DATE_FORMAT_1 = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	public static final String DATE_FORMAT_2 = "yyyy-MM-dd' 'HH:mm:ss.SSSSSS";
	public static final String DEFAULT_MEASUREMENT = "PROVEN_MEASUREMENT";
	public static final RDFNode provenMessageRes = ResourceFactory.createResource(PROVEN_MESSAGE_RES);
	public static final RDFNode provenMeasurementRes = ResourceFactory.createResource(PROVEN_MEASUREMENT_RES);
	public static final RDFNode provenQueryFilterRes = ResourceFactory.createResource(PROVEN_QUERY_FILTER_RES);
	public static final Property rdfTypeProp = ResourceFactory.createProperty(RDF_TYPE_PROP);
	public static final Property queryTypeProp = ResourceFactory.createProperty(QUERY_TYPE_PROP);
	public static final Property messageContentProp = ResourceFactory.createProperty(MESSAGE_CONTENT_PROP);
	public static final Property nameProp = ResourceFactory.createProperty(NAME_PROP);
	public static final Property timestampProp = ResourceFactory.createProperty(TIMESTAMP_PROP);
	public static final Property queryMeasurementProp = ResourceFactory.createProperty(QUERY_MEASUREMENT_PROP);

	private final static MessageModel messageModel = MessageModel.getInstance();

	/**
	 * Prepends context file to a json message. The context provides a mapping
	 * of terms to IRI's allowing the json message to be used as json-ld.
	 * 
	 * @param jsonMessage
	 *            provided json message for which context will be appended.
	 * 
	 * @return the json message with context prepended.
	 */
	public static String prependContext(String jsonMessage) {
		String context = messageModel.getContext();
		String ret = jsonMessage.replaceFirst("\\{", "{" + context);
		return ret;
	}

	public static void initializeMessageDataModel(ProvenMessage pm, Model model) throws InvalidProvenMessageException {

		// Contains original BN resource as key and replacement resource as
		// value for subjects
		Map<Resource, Resource> resourceBNs = new HashMap<Resource, Resource>();

		Map<Resource, Integer> objectCount = new HashMap<Resource, Integer>();

		// Contains replacement statements
		Model replacementModel = ModelFactory.createDefaultModel();

		// Content type of message data model
		// Explicit is default
		pm.setMessageContent(MessageContent.Explicit);

		// Listing of statements
		StmtIterator iter = model.listStatements();

		while (iter.hasNext()) {

			Statement stmt = iter.nextStatement();

			Resource subject = stmt.getSubject();
			Property predicate = stmt.getPredicate();
			RDFNode object = stmt.getObject();
			boolean isAnonSubject = false;
			boolean isAnonObject = false;
			Resource replacementSubject = subject;
			RDFNode replacementObject = object;

			// Check for Message Content Type - only query and explicit
			// currently supported.
			// TODO add checks for other content types. Like query, should be a
			// simple property existence check.
			boolean contentTypeFound = false;
			if (!contentTypeFound) {
				if (predicate.equals(queryTypeProp)) {
					pm.setMessageContent(MessageContent.Query);
					contentTypeFound = true;
				}
			}

			if (subject.isAnon()) {
				isAnonSubject = true;
				if (resourceBNs.containsKey(subject)) {
					log.debug("SUBJECT FOUND BEFORE");
					replacementSubject = resourceBNs.get(subject);
				} else {
					replacementSubject = ResourceFactory.createResource(getBNReplacementURI());
					resourceBNs.put(subject, replacementSubject);
					objectCount.put(replacementSubject, 0);
				}
			}

			if (object instanceof Resource) {
				if (object.isAnon()) {
					isAnonObject = true;
					Resource objectBN = (Resource) object;
					if (resourceBNs.containsKey(objectBN)) {
						log.debug("OBJECT FOUND BEFORE");
						replacementObject = resourceBNs.get(objectBN);
						objectCount.put((Resource) replacementObject, objectCount.get(replacementObject) + 1);
					} else {
						replacementObject = ResourceFactory.createResource(getBNReplacementURI());
						resourceBNs.put(objectBN, (Resource) replacementObject);
						objectCount.put((Resource) replacementObject, 1);
					}
				}
			}

			if (isAnonSubject || isAnonObject) {

				// Create new statement and add to replacement model
				replacementModel.add(ResourceFactory.createStatement(replacementSubject, predicate, replacementObject));

				// Remove statement with blank node(s)
				iter.remove();
			}
		}

		// Add content type and proven message type statements to message
		boolean pmStatementsAdded = false;
		for (Resource resource : objectCount.keySet()) {
			if (objectCount.get(resource).equals(0)) {
				log.debug("FOUND ROOT NODE");
				replacementModel.add(ResourceFactory.createStatement(resource, rdfTypeProp, provenMessageRes));
				replacementModel.add(ResourceFactory.createStatement(resource, messageContentProp,
						ResourceFactory.createPlainLiteral(pm.getMessageContent().toString())));
				pmStatementsAdded = true;
				break;
			}
		}

		// Must throw an exception if root node not found for some reason
		if (!pmStatementsAdded) {
			throw new InvalidProvenMessageException("Message root object not found.");
		}

		// Add back in statements w/o BNs
		model.add(replacementModel);

	}

	public static void listStatements(Model model) {

		StmtIterator iter = model.listStatements();

		// print out the predicate, subject and object of each statement
		while (iter.hasNext()) {
			Statement stmt = iter.nextStatement(); // get next statement
			Resource subject = stmt.getSubject(); // get the subject
			Property predicate = stmt.getPredicate(); // get the predicate
			RDFNode object = stmt.getObject(); // get the object

			if (subject.isAnon()) {
				System.out.print("$$  " + subject.toString() + "  $$");
			} else {
				System.out.print(subject.toString());
			}

			System.out.print(" " + predicate.toString() + " ");
			if (object instanceof Resource) {
				if (object.isAnon()) {
					System.out.print("$$  " + object.toString() + "  $$");
				} else {
					System.out.print(object.toString());
				}
			} else {
				// object is a literal
				System.out.print(" \"" + object.toString() + "\"");
			}

			System.out.println(" .");
		}
	}

	public static Resource getProvenMessageResource(UUID id) {
		// Create proven message resource
		String pmResStr = PROVEN_MESSAGE_NS + "ProvenMessage_" + id.toString();
		return ResourceFactory.createResource(pmResStr);
	}

	public static Model createMessageDataModel(ProvenMessage pm, String jsonld) throws InvalidProvenMessageException {

		Model dataModel;

		try (InputStream in = new ByteArrayInputStream(jsonld.getBytes())) {

			dataModel = ModelFactory.createDefaultModel();
			RDFDataMgr.read(dataModel, in, RDFLanguages.JSONLD);
			initializeMessageDataModel(pm, dataModel);

		} catch (Exception e) {
			throw new InvalidProvenMessageException("Failed to create message data model.", e);
		}

		return dataModel;
	}

	public static Model addHierarchies(Model model) {

		// TODO Should shapesModel be an ONT model for simple owl inferencing ?
		// YES
		// Simple OWL classification reasoning
		// create new ont model w/simple reasoning
		// union w/shapes model to create new shapes model
		// Model ontModel =
		// ModelFactory.createOntologyModel(OntModelSpec.OWL_LITE_MEM_TRANS_INF);
		// ontModel.add(messageModel.getOntologyModel());
		//
		// Graph[] graphsArray = new Graph[2];
		// graphsArray[0] = model.getGraph();
		// graphsArray[1] = messageModel.getOntologyModel().getGraph();
		// MultiUnion ontGraph = new MultiUnion(graphsArray);
		// Reasoner microReasoner = ReasonerRegistry.getOWLReasoner();
		// microReasoner.bindSchema(ontGraph);
		// MessageUtils.listStatements(microReasoner.getReasonerCapabilities());
		// InfModel infmodel = ModelFactory.createInfModel(microReasoner,
		// model);
		// MessageUtils.listStatements(infmodel);

		// Reasoner microReasoner = ReasonerRegistry.getOWLReasoner();
		// microReasoner.bindSchema(messageModel.getOntologyModel());
		// InfModel infmodel = ModelFactory.createInfModel(microReasoner,
		// model);
		// MessageUtils.listStatements(infmodel);

		return null;
	}

	public static Model addShaclRuleResults(Model dataModel) {

		Model shapesModel = messageModel.getShapesModel();
		Model ontologyModel = messageModel.getOntologyModel();
		Model dataAndOntologyModel = dataModel.union(ontologyModel);

		Model results = RuleUtil.executeRules(dataModel, shapesModel, null, null);
		return dataModel.union(results);
	}

	/**
	 * Converts Jena model statements to a collection of
	 * {@link ProvenStatement}. This conversion assumes that no blank nodes
	 * exist in the source Jena Model.
	 * 
	 * @param model
	 *            the Jena Model to be converted into Proven statements
	 * @return a collection of Proven statements
	 * @throws InvalidProvenStatementsException
	 *             if Proven statements could not be created
	 */
	public static Collection<ProvenStatement> getProvenStatements(Model model) throws InvalidProvenStatementsException {

		Collection<ProvenStatement> pStmts = new ArrayList<ProvenStatement>();
		StmtIterator iter = model.listStatements();

		while (iter.hasNext()) {

			Statement jStmt = iter.nextStatement();
			Resource jSubject = jStmt.getSubject();
			Property jPredicate = jStmt.getPredicate();
			RDFNode jObject = jStmt.getObject();

			URI pSubject;
			URI pPredicate;
			try {
				pSubject = new URI(jSubject.toString());
				pPredicate = new URI(jPredicate.toString());
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				throw new InvalidProvenStatementsException("Failed to convert to proven statements", e);
			}
			String pObject = jObject.toString();
			ProvenStatement.ObjectValueType objectValueType = ProvenStatement.ObjectValueType.Literal;
			if (jObject.isResource()) {
				objectValueType = ProvenStatement.ObjectValueType.URI;
			}
			pStmts.add(new ProvenStatement(pSubject, pPredicate, pObject, objectValueType));
		}
		return pStmts;
	}

	public static Collection<ProvenMeasurement> getProvenMeasurements(Model model)
			throws InvalidProvenMeasurementException {

		Collection<ProvenMeasurement> ret = new ArrayList<ProvenMeasurement>();

		Graph modelGraph = model.getGraph();

		try {

			// Get Proven message
			URI provenMessage = getProvenMessage(modelGraph);

			// Get Measurements
			ExtendedIterator<Triple> measurementIter = modelGraph.find(Node.ANY, rdfTypeProp.asNode(),
					provenMeasurementRes.asNode());
			while (measurementIter.hasNext()) {
				URI provenMessageMeasurement = new URI(measurementIter.next().getSubject().toString());
				ProvenMeasurement pm = new ProvenMeasurement();
				pm.setProvenMessageMeasurement(provenMessageMeasurement);
				pm.setProvenMessage(provenMessage);
				ret.add(pm);
			}

			// Get Measurement properties
			// Add any metrics (i.e. TAG or FIELD literal datatypes)
			// Add Measurement name (hasName)
			// Add time stamp (hasTimestamp)
			for (ProvenMeasurement pm : ret) {

				Resource mResource = ResourceFactory.createResource(pm.provenMessageMeasurement.toString());
				ExtendedIterator<Triple> mPropIter = modelGraph.find(mResource.asNode(), Node.ANY, Node.ANY);
				while (mPropIter.hasNext()) {

					Triple t3 = mPropIter.next();
					log.debug(t3.toString());

					Node tSubject = t3.getSubject();
					Node tPredicate = t3.getPredicate();
					Node tObject = t3.getObject();

					if (tObject.isLiteral()) {

						// ProvenMetric
						ProvenMetric metric = ProvenMetric.MetricFragmentIdentifier.buildProvenMetric(t3);
						if (null != metric) {
							pm.addMetric(metric);
						}

						// hasName
						if (tPredicate.equals(nameProp.asNode())) {
							pm.setMeasurementName(tObject.getLiteral().getLexicalForm());
						}

						// hasTimestamp (long epoch format is standard for
						// proven messaging)
						if (tPredicate.equals(timestampProp.asNode())) {
							String dateStr = tObject.getLiteral().getLexicalForm();
							pm.setTimestamp(Long.valueOf(dateStr));
						}

					}
				}
			}

		} catch (Exception e) {
			throw new InvalidProvenMeasurementException("Failed to convert proven measurements", e);
		}

		return ret;
	}

	public static Long convertDateTimeStr(String dateStr) {

		Long ret = null;

		// Format 1
		if (null == ret) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_1);
				ret = sdf.parse(dateStr).getTime();
			} catch (Exception e) {
				ret = null;
			}
		}

		// Format 2
		if (null == ret) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_2);
				ret = sdf.parse(dateStr).getTime();
			} catch (Exception e) {
				ret = null;
			}
		}

		if (null == ret) {
			log.warn("Invalid date time string provided in message: " + dateStr);
		}

		return ret;
	}

	public static ProvenQueryTimeSeries getProvenQuery(Model model) throws InvalidProvenQueryException {

		ProvenQueryTimeSeries tsq = new ProvenQueryTimeSeries();

		Graph modelGraph = model.getGraph();

		try {

			// Get Proven message and create query object
			URI provenMessage = getProvenMessage(modelGraph);
			tsq.setProvenMessage(provenMessage);

			
			// Get measurement name
			URI measUri = null;
			ExtendedIterator<Triple> measIter = 
					modelGraph.find(Node.ANY,queryMeasurementProp.asNode(), Node.ANY);
			while (measIter.hasNext()) {
                tsq.setMeasurementName(measIter.next().getObject().getLiteral().getLexicalForm());
			}
			
			// Get Query Filter object, assumption is a single Query Filter
			// object with one or more Query Filter properties.
			URI pqfUri = null;
			// ProvenQueryFilter
			ExtendedIterator<Triple> qfIter = modelGraph.find(Node.ANY, rdfTypeProp.asNode(),
					provenQueryFilterRes.asNode());
			while (qfIter.hasNext()) {
				pqfUri = new URI(qfIter.next().getSubject().toString());
				break;
			}

			// Get Query Filters
			if (null != pqfUri) {

				Resource qfResource = ResourceFactory.createResource(pqfUri.toString());
				ExtendedIterator<Triple> qfPropIter = modelGraph.find(qfResource.asNode(), Node.ANY, Node.ANY);
				while (qfPropIter.hasNext()) {

					Triple t3 = qfPropIter.next();
					log.debug(t3.toString());

					Node tSubject = t3.getSubject();
					Node tPredicate = t3.getPredicate();
					Node tObject = t3.getObject();

					if (tObject.isLiteral()) {

						String field = tPredicate.getLocalName().toString();
						String tObjectDatatype = tObject.getLiteralDatatype().getURI();
						String datatype;
						if(tObjectDatatype.contains("::")) {
							datatype = tObjectDatatype.split("::")[1];
						}
						else
							datatype = tObjectDatatype.split("#")[1];
						String value = tObject.getLiteral().getLexicalForm();
						ProvenQueryFilter pqf = new ProvenQueryFilter();
						pqf.setField(field);
						pqf.setValue(value);
						pqf.setDatatype(datatype);
						tsq.addFilter(pqf);

					}

				}

			}

			if (null == tsq.getFilters()) {
				throw new InvalidProvenQueryException("Unfiltered time-series queries are not supported.");
			}

		} catch (Exception e) {
			throw new InvalidProvenQueryException("Failed to convert to proven query", e);
		}

		return tsq;
	}

	private static URI getProvenMessage(Graph messageGraph) throws URISyntaxException, InvalidProvenMessageException {

		// Get Proven message
		URI provenMessage = null;
		ExtendedIterator<Triple> provenMessageIter = messageGraph.find(Node.ANY, rdfTypeProp.asNode(),
				provenMessageRes.asNode());
		while (provenMessageIter.hasNext()) {
			provenMessage = new URI(provenMessageIter.next().getSubject().toString());
			provenMessageIter.close();
			break;
		}

		if (null == provenMessage) {
			throw new InvalidProvenMessageException("Missing ProvenMessage concept in message graph");
		}

		return provenMessage;
	}

	private static String getBNReplacementURI() {
		return PROVEN_MESSAGE_NS + BlankNodeId.create();
	}

}
