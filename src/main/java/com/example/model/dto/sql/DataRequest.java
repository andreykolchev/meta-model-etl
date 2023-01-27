package com.example.model.dto.sql;

import lombok.Data;

import java.util.List;

/**
 * @author : Andrey Kolchev
 * @since : 07/03/2020
 */
@Data
public class DataRequest {
    String name;
    String mainTable;
    List<DsTable> tables;
}
