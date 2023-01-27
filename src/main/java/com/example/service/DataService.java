package com.example.service;

import com.example.model.dto.sql.DataRequest;
import com.example.model.dto.sql.DataResponse;
import com.example.model.dto.sql.DsField;
import com.example.model.dto.sql.DsTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.factory.DataContextFactoryRegistryImpl;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.apache.metamodel.schema.Column;
import org.jooq.*;
import org.jooq.impl.DefaultDataType;
import org.springframework.stereotype.Service;

import javax.management.RuntimeErrorException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.*;

/**
 * @author : Andrey Kolchev
 * @since : 07/16/2020
 */
@Service
@Slf4j
public class DataService {

    final String DIM = "DIM_";
    final String FACT = "FACT_";
    final SQLDialect H2 = SQLDialect.H2;
    private DSLContext dsl;


    public DataResponse buildDataSet(String sessionId, DataRequest rq) {
//        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:" + sessionId + ";DB_CLOSE_DELAY=-1", "sa", "");) {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:" + sessionId, "sa", "");) {
            dsl = using(connection, H2);
            for (DsTable table : rq.getTables()) {
                processSourceTable(table);
            }
            processFactTable(rq);
            return fetchResult(rq);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private void processSourceTable(DsTable dsTable) {
        //init dimension table
        List<DsField> dsFields = dsTable.getFields();
        Table<?> dimTable = table(DIM + dsTable.getName());
        List<Field<?>> dimFields = new LinkedList<>();
        for (DsField dsField : dsFields) {
            Field<?> field = field(dsField.getName(), DefaultDataType.getDataType(H2, dsField.getType()));
            dimFields.add(field);
        }
        CreateTableColumnStep create = dsl.createTable(dimTable).columns(dimFields);
        log.info(create.getSQL());
        create.execute();

        //data loading from source to dimension
        final DataContextPropertiesImpl metaProperties = new DataContextPropertiesImpl();
        dsTable.getParameters().forEach(metaProperties::put);
        DataContext metaDsl = DataContextFactoryRegistryImpl.getDefaultInstance().createDataContext(metaProperties);
        org.apache.metamodel.schema.Table metaTable = metaDsl.getDefaultSchema().getTable(0);
        List<Column> metaColumns = new LinkedList<>();
        for (DsField dsField : dsFields) {
            metaColumns.add(metaTable.getColumnByName(dsField.getName()));
        }
        log.info(metaDsl.query().from(metaTable).select(metaColumns).compile().toSql());
        DataSet metaDataSet = metaDsl.query().from(metaTable).select(metaColumns).execute();
        while (metaDataSet.next()) {
            dsl.insertInto(dimTable).columns(dimFields).values(metaDataSet.getRow().getValues()).execute();
        }
    }

    private void processFactTable(DataRequest rq) {
        List<DsTable> dsTables = rq.getTables();
        final String mainTableName = rq.getMainTable();
        final String factTableName = FACT + rq.getName();
        Table<?> factTale = table(factTableName);
        List<Field<?>> factFields = new LinkedList<>();
        for (DsTable dsTable : dsTables) {
            String keyFieldName = dsTable.getKey();
            String keyFieldType = dsTable.getFields().stream()
                    .filter(field -> keyFieldName.equals(field.getName()))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeErrorException(new Error("No key field!")))
                    .getType();
            factFields.add(field(keyFieldName, DefaultDataType.getDataType(H2, keyFieldType)));
        }
        CreateTableColumnStep create = dsl.createTable(factTale).columns(factFields);
        log.info(create.getSQL());
        create.execute();

        //merge into fact
        final String mainKeyField = dsTables.stream()
                .filter(dsTable -> mainTableName.equals(dsTable.getName()))
                .findFirst()
                .orElseThrow(() -> new RuntimeErrorException(new Error("No key field!")))
                .getKey();
        List<Field<?>> dimFields = dsTables.stream()
                .map(dsTable -> field(DIM + dsTable.getName() + "." + dsTable.getKey()))
                .collect(Collectors.toList());
        SelectJoinStep<Record> joinStep = dsl.select(dimFields).from(DIM + mainTableName);
        dsTables.stream()
                .filter(dsTable -> !mainTableName.equals(dsTable.getName()))
                .forEach(dsTable -> joinStep.innerJoin(table(DIM + mainTableName)).on(dsTable.getBinding()));
        Merge merge = dsl.mergeInto(factTale).columns(factFields).key(field(mainKeyField)).select(joinStep);
        log.info(merge.getSQL());
        merge.execute();
    }

    private DataResponse fetchResult(DataRequest rq) {
        List<DsTable> dsTables = rq.getTables();
        final String factTableName = FACT + rq.getName();
        Table<Record> factTable = table(factTableName);
        List<Field<?>> factFields = new LinkedList<>();
        for (DsTable dsTable : dsTables) {
            dsTable.getFields().stream()
                    .map(dsField -> DIM + dsTable.getName() + "." + dsField.getName())
                    .forEach(fieldName -> factFields.add(field(fieldName)));
        }
        SelectJoinStep<Record> select = dsl.select(factFields).from(factTable);
        for (DsTable dsTable : dsTables) {
            String keyField = dsTable.getKey();
            String dimTableName = DIM + dsTable.getName();
            String bindingSql = factTableName + "." + keyField + "=" + dimTableName + "." + keyField;
            select.innerJoin(table(dimTableName)).on(bindingSql);
        }
        log.info(select.getSQL());
        Result<Record> records = select.fetch();
        List<Map<String, Object>> result = new LinkedList<>();
        for (Record record : records) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (Field<?> field : record.fields()) {
                row.put(field.getName(), record.get(field));
            }
            result.add(row);
        }
        return new DataResponse(result);
    }


}
