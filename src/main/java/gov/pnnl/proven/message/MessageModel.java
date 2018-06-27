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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.compose.MultiUnion;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.topbraid.shacl.util.SHACLSystemModel;

import gov.pnnl.proven.message.exception.MissingMessageModelContextException;
import gov.pnnl.proven.message.exception.MissingMessageModelOntologyException;
import gov.pnnl.proven.message.exception.MissingMessageModelShapesException;
import gov.pnnl.proven.message.exception.MultipleMessageModelContextException;

import static gov.pnnl.proven.message.MessageModelFile.ModelConfig.*;

/**
 * Provides access to the message model, used to construct and validate a
 * {@code ProvenMessage}. The message model provides a JSON-LD context mapping
 * terms to model concepts, ontology files describing class concept structures,
 * and SHACL Shapes files providing validation and inference rules.
 * 
 * @author d3j766
 *
 */
public class MessageModel {

	private static Logger log = LoggerFactory.getLogger(MessageModel.class);

	public static final String MESSAGE_MODEL_PATH = "message-model/";

	private static MessageModel instance;

	private MessageModel() {
		log.debug("Loading message model...");
		loadMessageModel();
	}

	static {
		try {
			instance = new MessageModel();
		} catch (MessageModelInstanceException ex) {
			throw ex;
		}
	}

	/**
	 * Raw model files. See the different {@link MessageModelFile} types. Each
	 * file is stored as a string. Assumption is these are "small" and not many.
	 * Key is the file name and value is the file contents. Assumption is a
	 * single resource folder contains all model files making the resource name
	 * unique and why it is used as the key.
	 */
	private Map<String, String> rawModelFiles = new HashMap<String, String>();

	/**
	 * JSON-LD context definition used to describe/map term values to IRI's
	 */
	private String context;

	/**
	 * Ontology model.
	 */
	private Model ontologyModel;

	/**
	 * SHACL shapes model, union of both ontology (i.e. OWL/RDF) and shapes
	 * files.
	 */
	private Model shapesModel;

	/**
	 * Exception creating MesssageModel instance. Is a runtime exception
	 * indicating recovery is not possible.
	 * 
	 * @author d3j766
	 *
	 */
	private class MessageModelInstanceException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public MessageModelInstanceException(String message) {
			super(message);
		}

		public MessageModelInstanceException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	public static MessageModel getInstance() {
		return instance;
	}

	public String getContext() {
		return context;

	}

	public Model getOntologyModel() {
		return ontologyModel;
	}

	public Model getShapesModel() {
		return shapesModel;
	}

	public String getModelFile(String resourceName) throws Exception {

		String ret = null;

		String resourcePath = getModelResourcePath(resourceName);

		try (InputStream resourceIn = this.getClass().getClassLoader().getResourceAsStream(resourcePath)) {

			List<String> contents = IOUtils.readLines(resourceIn, Charset.defaultCharset());

			StringBuilder sb = new StringBuilder();
			for (String line : contents) {
				sb.append(line);
			}

			ret = sb.toString();

		} catch (Exception e) {
			throw new MessageModelInstanceException("Failed to load model file:" + resourcePath, e);
		}

		return ret;
	}

	private void loadMessageModel() {
		loadRawModelFiles();
		loadContext();
		loadModels();
	}

	/**
	 * Loads files identified in registry file. A model must have exactly one
	 * context file and at least one shapes file.
	 */
	private void loadRawModelFiles() {

		String resourcePath = getModelResourcePath(MODEL_REGISTRY_FILE);

		try (InputStream resourceIn = this.getClass().getClassLoader().getResourceAsStream(resourcePath)) {

			// Read file list from registry
			List<String> resources = IOUtils.readLines(resourceIn, Charset.defaultCharset());

			log.debug("After file lookup..." + resources);

			int contextCnt = 0;
			int shapesCnt = 0;
			int ontologyCnt = 0;
			for (String resource : resources) {

				log.debug("model file name: " + resource);

				// Must be recognized as a MessageModelFile type
				MessageModelFile mmf = MessageModelFile.modelFileType(resource);
				if (null != mmf) {

					if (mmf == MessageModelFile.CONTEXT) {
						contextCnt++;
					}

					if ((mmf == MessageModelFile.ONTOLOGY)) {
						ontologyCnt++;
					}

					if ((mmf == MessageModelFile.SHAPES)) {
						shapesCnt++;
					}

					String modelFile = getModelFile(resource);
					rawModelFiles.put(resource, modelFile);
				}
			}

			log.debug("Raw files loaded: " + rawModelFiles.size());

			// Ensure there is a context file
			if (contextCnt == 0) {
				throw new MissingMessageModelContextException();
			}

			// Ensure there are not multiple context files
			if (contextCnt > 1) {
				throw new MultipleMessageModelContextException();
			}

			// Ensure there is an ontology file
			if (ontologyCnt < 1) {
				throw new MissingMessageModelOntologyException();
			}

			// Ensure there is a shapes file
			if (shapesCnt < 1) {
				throw new MissingMessageModelShapesException();
			}

		} catch (Exception e) {
			throw new MessageModelInstanceException("Failed to load raw model files.", e);
		}
	}

	private void loadContext() {
		for (String resourceName : rawModelFiles.keySet()) {
			if (isContext(resourceName)) {
				context = rawModelFiles.get(resourceName);
			}
		}
	}

	private void loadModels() {

		shapesModel = ModelFactory.createDefaultModel();
		ontologyModel = ModelFactory.createDefaultModel();

		List<Graph> shapesGraphs = new ArrayList<Graph>();
		List<Graph> ontologyGraphs = new ArrayList<Graph>();
		for (String resourceName : rawModelFiles.keySet()) {

			if ((isShapes(resourceName) || isOntology(resourceName))) {

				String modelStr = rawModelFiles.get(resourceName);

				Model model;
				try (InputStream in = new ByteArrayInputStream(modelStr.getBytes())) {

					model = ModelFactory.createDefaultModel();
					RDFDataMgr.read(model, in, RDFLanguages.JSONLD);

				} catch (Exception e) {
					throw new MessageModelInstanceException("Failed to create shapes and ontology models.", e);
				}

				// shapesModel = shapesModel.union(model);

				if (isOntology(resourceName)) {
					ontologyModel.add(model);
					shapesGraphs.add(model.getGraph());
					ontologyGraphs.add(model.getGraph());
				}

				if (isShapes(resourceName)) {
					shapesModel.add(model);
					shapesGraphs.add(model.getGraph());
				}

			}
		}

		// Add SHACL model
		Model unionModel = SHACLSystemModel.getSHACLModel();
		MultiUnion unionGraph = new MultiUnion(new Graph[] { unionModel.getGraph(), shapesModel.getGraph() });
		shapesModel = ModelFactory.createModelForGraph(unionGraph);

	}

	/**
	 * Build path for a provided resource name.
	 * 
	 * @param resourceName
	 * @return resource path
	 */
	private String getModelResourcePath(String resourceName) {
		return MESSAGE_MODEL_PATH + resourceName;
	}

	/**
	 * Based on the resource name, determines if the file contains a JSON-LD
	 * context definition.
	 * 
	 * @param resource
	 * @return true if resource is a context file, false otherwise.
	 */
	private boolean isContext(String resourceName) {
		return (MessageModelFile.modelFileType(resourceName) == MessageModelFile.CONTEXT);
	}

	/**
	 * Based on the resource name, determines if the file is an OWL/RDF ontolgy
	 * file.
	 * 
	 * @param resource
	 * @return true if resource is an OWL/RDF ontology file, false otherwise.
	 */
	private boolean isOntology(String resourceName) {
		return (MessageModelFile.modelFileType(resourceName) == MessageModelFile.ONTOLOGY);
	}

	/**
	 * Based on the resource name, determines if the file is a SHACL shapes
	 * file.
	 * 
	 * @param resource
	 * @return true if resource is a shapes file, false otherwise.
	 */
	private boolean isShapes(String resourceName) {
		return (MessageModelFile.modelFileType(resourceName) == MessageModelFile.SHAPES);
	}

}
