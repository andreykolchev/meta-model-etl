package com.example.model.dto.sql;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author : Andrey Kolchev
 * @since : 07/03/2020
 */
@Data
@AllArgsConstructor
public class DataResponse {
    List<Map<String, Object>> data;
}
