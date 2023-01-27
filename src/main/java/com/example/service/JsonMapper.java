package com.example.service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.MapType;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

@Component
public class JsonMapper {
    private static final String FILE = "File: '";
    private final ObjectMapper mapper;

    public JsonMapper(final ObjectMapper objectMapper) {
        this.mapper = objectMapper;
    }

    public <T> String toJson(final T object) {
        Objects.requireNonNull(object);
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T toObject(final Class<T> clazz, final String json) {
        Objects.requireNonNull(json);
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public <T> T toObject(final Class<T> clazz, final JsonNode json) {
        Objects.requireNonNull(json);
        try {
            return mapper.treeToValue(json, clazz);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public JsonNode toJsonNode(final String json) {
        Objects.requireNonNull(json);
        try {
            return mapper.readTree(json);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public <T> JsonNode toJsonNode(final T object) {
        Objects.requireNonNull(object);
        return mapper.valueToTree(object);
    }

    public ObjectNode createObjectNode() {
        return mapper.createObjectNode();
    }

    public ArrayNode createArrayNode() {
        return mapper.createArrayNode();
    }

    public <T> T toMap(final MapType mapType, final String json) {
        Objects.requireNonNull(json);
        try {
            return mapper.readValue(json, mapType);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getResource(final String fileName) {
        final String resource = readResource(fileName);
        try {
            return toCompact(resource);
        } catch (IOException e) {
            throw new IllegalArgumentException("Error in file: '" + fileName + "'.");
        }
    }

    private String readResource(final String fileName) {
        final String path = getPathFile(fileName);
        return read(path);
    }

    private String getPathFile(final String fileName) {
        return Optional.ofNullable(getClass().getClassLoader().getResource(fileName))
                .map(URL::getPath)
                .orElseThrow(() -> new IllegalArgumentException(FILE + fileName + "' not found."));
    }

    private String read(final String pathToFile) {
        try {
            final Path path = Paths.get(pathToFile);
            final byte[] buffer = Files.readAllBytes(path);
            return new String(buffer);
        } catch (IOException | InvalidPathException e) {
            throw new IllegalArgumentException(FILE + pathToFile + "' can not be readData.");
        }
    }

    private String toCompact(final String source) throws IOException {
        final JsonFactory factory = new JsonFactory();
        final JsonParser parser = factory.createParser(source);
        final StringWriter out = new StringWriter();
        try (JsonGenerator gen = factory.createGenerator(out)) {
            while (parser.nextToken() != null) {
                gen.copyCurrentEvent(parser);
            }
        }
        return out.getBuffer().toString();
    }

    public String merge(final String mainJson, final String updateJson) {
        Objects.requireNonNull(mainJson);
        Objects.requireNonNull(updateJson);
        try {
            final JsonNode mainNode = mapper.readTree(mainJson);
            final JsonNode updateNode = mapper.readTree(updateJson);
            final JsonNode mergedJson = merge(mainNode, updateNode);
            return mapper.writeValueAsString(mergedJson);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public JsonNode merge(final JsonNode mainNode, final JsonNode updateNode) {

        final Iterator<String> fieldNames = updateNode.fieldNames();

        while (fieldNames.hasNext()) {
            final String updatedFieldName = fieldNames.next();
            final JsonNode valueToBeUpdated = mainNode.get(updatedFieldName);
            final JsonNode updatedValue = updateNode.get(updatedFieldName);
            // If the node is an @ArrayNode
            if (valueToBeUpdated != null && valueToBeUpdated.isArray() && updatedValue.isArray()) {
                // running a loop for all elements of the updated ArrayNode
                for (int i = 0; i < updatedValue.size(); i++) {
                    final JsonNode updatedChildNode = updatedValue.get(i);
                    // Create a new Node in the node that should be updated, if there was no corresponding node in it
                    // Use-case - where the updateNode will have a new element in its Array
                    if (valueToBeUpdated.size() <= i) {
                        ((ArrayNode) valueToBeUpdated).add(updatedChildNode);
                    }
                    // getting reference for the node to be updated
                    final JsonNode childNodeToBeUpdated = valueToBeUpdated.get(i);
                    merge(childNodeToBeUpdated, updatedChildNode);
                }
                // if the Node is an @ObjectNode
            } else if (valueToBeUpdated != null && valueToBeUpdated.isObject()) {
                merge(valueToBeUpdated, updatedValue);
            } else {
                if (mainNode instanceof ObjectNode) {
                    ((ObjectNode) mainNode).replace(updatedFieldName, updatedValue);
                }
            }
        }
        return mainNode;
    }
}
