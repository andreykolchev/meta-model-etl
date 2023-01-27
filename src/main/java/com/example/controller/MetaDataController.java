package com.example.controller;

import com.example.model.dto.metamodel.*;
import com.example.service.JsonMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.factory.DataContextFactoryRegistryImpl;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/meta-data")
public class MetaDataController {

    final JsonMapper jsonMapper;

    public MetaDataController(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }


    @PostMapping("/source")
    public List<Map<String, Object>> buildDataSet(@RequestBody SourceParamsRq rq) {
        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();


        List<Map<String, Object>> result = new ArrayList<>();
        return result;
    }

    @GetMapping("/jdbc")
    public JdbcMetaDataRs jdbc() {
        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
        properties.put("type", "jdbc");
        properties.put("url", "jdbc:h2:mem:source");
        properties.put("driverClassName", "org.h2.Driver");
        properties.put("username", "sa");
        properties.put("password", "");
        DataContext dataContext = DataContextFactoryRegistryImpl.getDefaultInstance().createDataContext(properties);
        List<SchemaRs> schemas = new ArrayList<>();
        for (Schema schema : dataContext.getSchemas()) {
            List<TableRs> tables = new ArrayList<>();
            for (Table table : schema.getTables()) {
                List<ColumnRs> columns = new ArrayList<>();
                for (Column column : table.getColumns()) {
                    columns.add(new ColumnRs(column.getName(), column.getNativeType()));
                }
                tables.add(new TableRs(table.getName(), columns));
            }
            schemas.add(new SchemaRs(schema.getName(), tables));
        }
        return new JdbcMetaDataRs(schemas);
    }

    @GetMapping("/csv")
    public CsvMetaDataRs csv() {
        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
        properties.put("type", "csv");
        properties.put("resource", "/home/user/IdeaProject/ddd/metaModel/src/main/resources/data/parties.csv");
        DataContext dataContext = DataContextFactoryRegistryImpl.getDefaultInstance().createDataContext(properties);
        List<String> tableNames = dataContext.getDefaultSchema().getTableNames();
        System.out.println(Arrays.toString(tableNames.toArray()));
        final String table = tableNames.get(0);
        DataSet dataSet = dataContext.query()
                .from(table)
                .select("*")
                .execute();
        List<ColumnRs> columns = new ArrayList<>();
        dataSet.getSelectItems().forEach(selectItem -> {
            Column column = selectItem.getColumn();
            columns.add(new ColumnRs(column.getName(), "STRING"));
        });
        return new CsvMetaDataRs(new TableRs(table, columns));
    }

    @GetMapping("/json")
    public CsvMetaDataRs json() {
        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
        properties.put("type", "json");
        properties.put("resource", "/home/user/IdeaProject/ddd/metaModel/src/main/resources/data/agreement.json");
        DataContext dataContext = DataContextFactoryRegistryImpl.getDefaultInstance().createDataContext(properties);
        List<String> tableNames = dataContext.getDefaultSchema().getTableNames();
        System.out.println(Arrays.toString(tableNames.toArray()));
        final String table = tableNames.get(0);
        DataSet dataSet = dataContext.query()
                .from(table)
                .select("*")
                .execute();
        List<ColumnRs> columns = new ArrayList<>();
        dataSet.getSelectItems().forEach(selectItem -> {
            Column column = selectItem.getColumn();
            columns.add(new ColumnRs(column.getName(), column.getType().getName()));
        });
        while (dataSet.next()) {
            for (Object value : dataSet.getRow().getValues()) {
                System.out.println(value.toString());
            }
        }
        return new CsvMetaDataRs(new TableRs(table, columns));
    }

    @GetMapping("/jsonMapper")
    public String jsonMapper() {
        String jsonString = jsonMapper.getResource("data/agreement.json");
        JsonNode json =  jsonMapper.toJsonNode(jsonString);
        JsonNode node =  json.elements().next();
        Iterator<String> fieldNames =  node.fieldNames();
        while (fieldNames.hasNext()) {
            System.out.println(fieldNames.next());
        }
        return "OK";
    }

}
