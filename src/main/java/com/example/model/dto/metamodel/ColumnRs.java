package com.example.model.dto.metamodel;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author : Andrey Kolchev
 * @since : 07/13/2020
 */
@Data
@AllArgsConstructor
public class ColumnRs {
    String name;
    String type;
}
