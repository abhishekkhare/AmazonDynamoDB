package com.abhi.edu.dynamoDB.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * 
 * @author Abhishek Khare
 *
 */
public class JacksonConverter {
	/**
	 * Maximum JSON depth.
	 */
	private static final int MAX_DEPTH = 50;

	public JacksonConverter() {
	}

	/**
	 * Asserts the depth is not greater than {@link #MAX_DEPTH}.
	 *
	 * @param depth
	 *            Current JSON depth
	 * @throws RuntimeException
	 *             Depth is greater than {@link #MAX_DEPTH}
	 */
	private void assertDepth(final int depth) throws RuntimeException {
		if (depth > MAX_DEPTH) {
			throw new RuntimeException("Max depth reached. The object/array has too much depth.");
		}
	}

	/**
	 * Gets an DynamoDB representation of a JsonNode.
	 *
	 * @param node
	 *            The JSON to convert
	 * @param depth
	 *            Current JSON depth
	 * @return DynamoDB representation of the JsonNode
	 * @throws RuntimeException
	 *             Unknown JsonNode type or JSON is too deep
	 */
	private AttributeValue getAttributeValue(final JsonNode node, final int depth) throws RuntimeException {
		assertDepth(depth);
		switch (node.asToken()) {
			case VALUE_STRING:
				return new AttributeValue().withS(node.textValue());
			case VALUE_NUMBER_INT:
			case VALUE_NUMBER_FLOAT:
				return new AttributeValue().withN(node.numberValue().toString());
			case VALUE_TRUE:
			case VALUE_FALSE:
				return new AttributeValue().withBOOL(node.booleanValue());
			case VALUE_NULL:
				return new AttributeValue().withNULL(true);
			case START_OBJECT:
				return new AttributeValue().withM(jsonObjectToMap(node, depth));
			case START_ARRAY:
				return new AttributeValue().withL(jsonArrayToList(node, depth));
			default:
				throw new RuntimeException("Unknown node type: " + node);
		}
	}

	/**
	 * Converts a DynamoDB attribute to a JSON representation.
	 *
	 * @param av
	 *            DynamoDB attribute
	 * @param depth
	 *            Current JSON depth
	 * @return JSON representation of the DynamoDB attribute
	 * @throws RuntimeException
	 *             Unknown DynamoDB type or JSON is too deep
	 */
	private JsonNode getJsonNode(final AttributeValue av, final int depth) throws RuntimeException {
		assertDepth(depth);
		if (av.getS() != null) {
			return JsonNodeFactory.instance.textNode(av.getS());
		} else if (av.getN() != null) {
			try {
				return JsonNodeFactory.instance.numberNode(Integer.parseInt(av.getN()));
			} catch (final NumberFormatException e) {
				// Not an integer
				try {
					return JsonNodeFactory.instance.numberNode(Float.parseFloat(av.getN()));
				} catch (final NumberFormatException e2) {
					// Not a number
					throw new RuntimeException(e.getMessage());
				}
			}
		} else if (av.getBOOL() != null) {
			return JsonNodeFactory.instance.booleanNode(av.getBOOL());
		} else if (av.getNULL() != null) {
			return JsonNodeFactory.instance.nullNode();
		} else if (av.getL() != null) {
			return listToJsonArray(av.getL(), depth);
		} else if (av.getM() != null) {
			return mapToJsonObject(av.getM(), depth);
		} else {
			throw new RuntimeException("Unknown type value " + av);
		}
	}

	/**
     * Converts a list of maps of AttributeValues to a JsonNode instance that represents the list of maps.
     *
     * @param items
     *            A list of maps of AttributeValues
     * @return A JsonNode instance that represents the converted JSON array.
     * @throws JacksonConverterException
     *             Error converting DynamoDB item to JSON
     */
	public JsonNode itemListToJsonArray(final List<Map<String, AttributeValue>> items) throws RuntimeException {
		if (items != null) {
			final ArrayNode array = JsonNodeFactory.instance.arrayNode();
			for (final Map<String, AttributeValue> item : items) {
				array.add(mapToJsonObject(item, 0));
			}
			return array;
		}
		throw new RuntimeException("Items cannnot be null");
	}

	/**
     * Converts a JSON array to a list of AttributeValues.
     *
     * @param array
     *            A JsonNode instance that represents the target JSON array.
     * @return A list of AttributeValues that represents the JSON array.
     * @throws JacksonConverterException
     *             if JsonNode is not an array
     */
	public List<AttributeValue> jsonArrayToList(final JsonNode node) throws RuntimeException {
		return jsonArrayToList(node, 0);
	}

	/**
	 * Helper method to convert a JsonArrayNode to a DynamoDB list.
	 *
	 * @param node
	 *            Array node to convert
	 * @param depth
	 *            Current JSON depth
	 * @return DynamoDB list representation of the array node
	 * @throws RuntimeException
	 *             JsonNode is not an array or depth is too great
	 */
	private List<AttributeValue> jsonArrayToList(final JsonNode node, final int depth) throws RuntimeException {
		assertDepth(depth);
		if (node != null && node.isArray()) {
			final List<AttributeValue> result = new ArrayList<AttributeValue>();
			final Iterator<JsonNode> children = node.elements();
			while (children.hasNext()) {
				final JsonNode child = children.next();
				result.add(getAttributeValue(child, depth));
			}
			return result;
		}
		throw new RuntimeException("Expected JSON array, but received " + node);
	}

	/**
     * Converts a JSON object to a map of AttributeValues.
     *
     * @param object
     *            A JsonNode instance that represents the target JSON object.
     * @return A map of AttributeValues that represents the JSON object.
     * @throws JacksonConverterException
     *             if JsonNode is not an object.
     */
	public Map<String, AttributeValue> jsonObjectToMap(final JsonNode node) throws RuntimeException {
		return jsonObjectToMap(node, 0);
	}

	/**
	 * Transforms a JSON object to a DynamoDB object.
	 *
	 * @param node
	 *            JSON object
	 * @param depth
	 *            Current JSON depth
	 * @return DynamoDB object representation of JSON
	 * @throws RuntimeException
	 *             JSON is not an object or depth is too great
	 */
	private Map<String, AttributeValue> jsonObjectToMap(final JsonNode node, final int depth)
			throws RuntimeException {
		assertDepth(depth);
		if (node != null && node.isObject()) {
			final Map<String, AttributeValue> result = new HashMap<String, AttributeValue>();
			final Iterator<String> keys = node.fieldNames();
			while (keys.hasNext()) {
				final String key = keys.next();
				result.put(key, getAttributeValue(node.get(key), depth + 1));
			}
			return result;
		}
		throw new RuntimeException("Expected JSON Object, but received " + node);
	}

	/**
	 * 
	 * @param json
	 * 				JSON in String format.
	 * @return DynamoDB object representation of JSON
	 * @throws Exception
	 */
	public Map<String, AttributeValue> stringToMap(String json) throws Exception {
		return jsonObjectToMap(stringToJsonNode(json),0);

	}

	/**
	 * 
	 * @param node
	 * 				JSON object
	 * @param depth
	 * 				Current JSON depth
	 * @return DynamoDB object representation of JSON
	 * @throws RuntimeException
	 */
	private Map<String, AttributeValueUpdate> jsonObjectToMapU(final JsonNode node, final int depth)
			throws RuntimeException {
		assertDepth(depth);
		if (node != null && node.isObject()) {
			final Map<String, AttributeValueUpdate> result = new HashMap<String,AttributeValueUpdate>();
			final Iterator<String> keys = node.fieldNames();
			while (keys.hasNext()) {
				final String key = keys.next();
				AttributeValue value = getAttributeValue(node.get(key),depth+1);
				result.put(key, new AttributeValueUpdate(value,AttributeAction.PUT));
			}
			return result;
		}
		throw new RuntimeException("Expected JSON Object, but received " + node);
	}


	/**
	 * 
	 * @param json
	 * 				JSON in String format.
	 * @return DynamoDB update object representation of JSON
	 * @throws Exception
	 */
	public Map<String, AttributeValueUpdate> stringToMapU(String json) throws Exception {
		return jsonObjectToMapU(stringToJsonNode(json),0);

	}

	/**
     * Converts a list of AttributeValues to a JsonNode instance that represents the list.
     *
     * @param list
     *            A list of AttributeValues
     * @return A JsonNode instance that represents the converted JSON array.
     * @throws JacksonConverterException
     *             Error converting DynamoDB item to JSON
     */
	public JsonNode listToJsonArray(final List<AttributeValue> item) throws RuntimeException {
		return listToJsonArray(item, 0);
	}

	/**
	 * Converts a DynamoDB list to a JSON list.
	 *
	 * @param item
	 *            DynamoDB list
	 * @param depth
	 *            Current JSON depth
	 * @return JSON array node representation of DynamoDB list
	 * @throws RuntimeException
	 *             Null DynamoDB list or JSON too deep
	 */
	private JsonNode listToJsonArray(final List<AttributeValue> item, final int depth) throws RuntimeException {
		assertDepth(depth);
		if (item != null) {
			final ArrayNode node = JsonNodeFactory.instance.arrayNode();
			for (final AttributeValue value : item) {
				node.add(getJsonNode(value, depth + 1));
			}
			return node;
		}
		throw new RuntimeException("Item cannot be null");
	}

	/**
     * Converts a map of AttributeValues to a JsonNode instance that represents the map.
     *
     * @param map
     *            A map of AttributeValues
     * @return A JsonNode instance that represents the converted JSON object.
     * @throws JacksonConverterException
     *             Error converting DynamoDB item to JSON
     */
	public JsonNode mapToJsonObject(final Map<String, AttributeValue> item) throws RuntimeException {
		return mapToJsonObject(item, 0);
	}

	/**
	 * Converts a DynamoDB object to a JSON map.
	 *
	 * @param item
	 *            DynamoDB object
	 * @param depth
	 *            Current JSON depth
	 * @return JSON map representation of the DynamoDB object
	 * @throws RuntimeException
	 *             Null DynamoDB object or JSON too deep
	 */
	@SuppressWarnings("deprecation")
	private JsonNode mapToJsonObject(final Map<String, AttributeValue> item, final int depth)
			throws RuntimeException {
		assertDepth(depth);
		if (item != null) {
			final ObjectNode node = JsonNodeFactory.instance.objectNode();

			for (final Entry<String, AttributeValue> entry : item.entrySet()) {
				node.put(entry.getKey(), getJsonNode(entry.getValue(), depth + 1));
			}
			return node;
		}
		throw new RuntimeException("Item cannot be null");
	}

	/**
	 * 
	 * @param json
	 * @return
	 * @throws Exception
	 */
	private JsonNode stringToJsonNode (final String json) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(json);
		return  actualObj;
	}

}

