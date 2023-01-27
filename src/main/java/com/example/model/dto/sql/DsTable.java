package com.example.model.dto.sql;

import lombok.Data;

import java.util.LinkedList;
import java.util.Map;

/**
 * @author : Andrey Kolchev
 * @since : 07/03/2020
 */
@Data
public class DsTable {
    String name;
    String binding;
    String key;
    Map<String, String> parameters;
    LinkedList<DsField> fields;
}
