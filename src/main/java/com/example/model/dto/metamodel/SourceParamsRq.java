package com.example.model.dto.metamodel;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author : Andrey Kolchev
 * @since : 07/03/2020
 */
@Data
public class SourceParamsRq {
    Map<String, String> parameters;
}
