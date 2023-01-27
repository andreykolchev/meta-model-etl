package com.example.service;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.factory.DataContextFactoryRegistryImpl;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : Andrey Kolchev
 * @since : 07/16/2020
 */
@Service
public class DataProcessingService {

    final String initScript;

    public DataProcessingService() {
        initScript = getInitScript("data/stageData.sql");
        initSourceDB();
    }

    public List<Map<String, Object>> buildDataSet(String sessionId, List<String> attributes) {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:" + sessionId, "sa", "");) {
            initTempStage(connection);
            processAccounts(connection);
            processParties(connection);
            processAgreements(connection);
            mergeAgreementFacts(connection);
            return executeQuery(connection, attributes);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private void initTempStage(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.addBatch(initScript);
        stmt.executeBatch();
        stmt.clearBatch();
    }

    private void processAccounts(Connection connection) throws SQLException {
        final String SQL_SELECT = "SELECT * FROM ACCOUNT";
        final String SQL_INSERT = "INSERT INTO DIM_ACCOUNT VALUES (?, ?, ?, ?)";
        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
        properties.put("type", "jdbc");
        properties.put("url", "jdbc:h2:mem:source");
        properties.put("driverClassName", "org.h2.Driver");
        properties.put("username", "sa");
        properties.put("password", "");
        DataContext dataContext = DataContextFactoryRegistryImpl.getDefaultInstance().createDataContext(properties);
        DataSet ds = dataContext.executeQuery(SQL_SELECT);
        while (ds.next()) {
            Row row = ds.getRow();
            PreparedStatement ps = connection.prepareStatement(SQL_INSERT);
            ps.setInt(1, (Integer) row.getValue(0));
            ps.setString(2, (String) row.getValue(1));
            ps.setString(3, (String) row.getValue(2));
            ps.setDate(4, new Date(System.currentTimeMillis()));
            ps.executeUpdate();
        }
    }

    private void processParties(Connection connection) throws SQLException {
        final String SQL_INSERT = "INSERT INTO DIM_COUNTERPARTY VALUES (?, ?, ?, ?)";
        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
        properties.put("type", "csv");
        properties.put("resource", "/home/user/IdeaProject/ddd/metaModel/src/main/resources/data/parties.csv");
        DataContext dataContext = DataContextFactoryRegistryImpl.getDefaultInstance().createDataContext(properties);
        List<String> tableNames = dataContext.getDefaultSchema().getTableNames();
        final String table = tableNames.get(0);
        DataSet ds = dataContext.query()
                .from(table)
                .select("*")
                .execute();
        while (ds.next()) {
            Row row = ds.getRow();
            PreparedStatement ps = connection.prepareStatement(SQL_INSERT);
            ps.setInt(1, Integer.valueOf((String) row.getValue(0)));
            ps.setString(2, (String) row.getValue(1));
            ps.setString(3, (String) row.getValue(2));
            ps.setDate(4, new Date(System.currentTimeMillis()));
            ps.executeUpdate();
        }
    }

    private void processAgreements(Connection connection) throws SQLException {
        final String SQL_INSERT = "INSERT INTO DIM_AGREEMENT VALUES (?, ?, ?, ?, ?, ?)";
        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
        properties.put("type", "json");
        properties.put("resource", "/home/user/IdeaProject/ddd/metaModel/src/main/resources/data/agreement.json");
        DataContext dataContext = DataContextFactoryRegistryImpl.getDefaultInstance().createDataContext(properties);
        List<String> tableNames = dataContext.getDefaultSchema().getTableNames();
        final String table = tableNames.get(0);
        DataSet ds = dataContext.query()
                .from(table)
                .select("agreementId, agreementDate, agreementDescription, counterPartyCode, partyCode")
                .execute();
        while (ds.next()) {
            Row row = ds.getRow();
            PreparedStatement ps = connection.prepareStatement(SQL_INSERT);
            ps.setInt(1, (Integer) row.getValue(0));
            ps.setString(2, (String) row.getValue(1));
            ps.setString(3, (String) row.getValue(2));
            ps.setString(4, (String) row.getValue(3));
            ps.setString(5, (String) row.getValue(4));
            ps.setDate(6, new Date(System.currentTimeMillis()));
            ps.executeUpdate();
        }
    }

    private void mergeAgreementFacts(Connection connection) throws SQLException {
        final String SQL_MERGE = "MERGE INTO FACT_AGREEMENT(" +
                "AGREEMENT_ID, " +
                "PARTY_KEY, " +
                "PARTY_ACCOUNT_KEY, " +
                "COUNTERPARTY_KEY, " +
                "COUNTERPARTY_ACCOUNT_KEY) KEY(AGREEMENT_ID) " +
                "SELECT a.AGREEMENT_ID," +
                " p.COUNTERPARTY_KEY," +
                " pda.ACCOUNT_KEY," +
                " cp.COUNTERPARTY_KEY," +
                " cpda.ACCOUNT_KEY " +
                "FROM DIM_AGREEMENT a " +
                "    INNER JOIN DIM_COUNTERPARTY p ON a.COUNTERPARTY_CODE = p.COUNTERPARTY_CODE " +
                "    INNER JOIN DIM_COUNTERPARTY cp ON a.PARTY_CODE = cp.COUNTERPARTY_CODE " +
                "    INNER JOIN DIM_ACCOUNT pda ON p.COUNTERPARTY_CODE = pda.COUNTERPARTY_CODE " +
                "    INNER JOIN DIM_ACCOUNT cpda ON cp.COUNTERPARTY_CODE = cpda.COUNTERPARTY_CODE ";
        Statement stmt = connection.createStatement();
        stmt.execute(SQL_MERGE);
    }

    private List<Map<String, Object>> executeQuery(Connection connection, List<String> attributes) throws SQLException {
        final String SQL_SELECT = " SELECT " + String.join(", ", attributes) +
                " FROM FACT_AGREEMENT FACT " +
                " INNER JOIN DIM_AGREEMENT AGREEMENT ON FACT.AGREEMENT_ID = AGREEMENT.AGREEMENT_ID" +
                " INNER JOIN DIM_COUNTERPARTY PARTY ON FACT.PARTY_KEY = PARTY.COUNTERPARTY_KEY" +
                " INNER JOIN DIM_COUNTERPARTY COUNTERPARTY ON FACT.COUNTERPARTY_KEY = COUNTERPARTY.COUNTERPARTY_KEY" +
                " INNER JOIN DIM_ACCOUNT PARTY_ACCOUNT ON FACT.PARTY_ACCOUNT_KEY = PARTY_ACCOUNT.ACCOUNT_KEY" +
                " INNER JOIN DIM_ACCOUNT COUNTERPARTY_ACCOUNT ON FACT.COUNTERPARTY_ACCOUNT_KEY = COUNTERPARTY_ACCOUNT.ACCOUNT_KEY;";
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(SQL_SELECT);
        ResultSetMetaData metaData = rs.getMetaData();
        List<Map<String, Object>> resultDataSet = new ArrayList<>();
        while (rs.next()) {
            HashMap<String, Object> row = new HashMap<>();
            for (int i = 1; i < metaData.getColumnCount(); i++) {
                row.put(metaData.getColumnName(i), rs.getObject(i));
            }
            resultDataSet.add(row);
        }
        return resultDataSet;
    }

    private String getInitScript(String resource) {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                Thread.currentThread().getContextClassLoader().getResourceAsStream(resource)));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                sb.append(line);
                sb.append(" ");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return sb.toString();
    }

    private void initSourceDB() {
        final String initDDL = getInitScript("data/source.sql");
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:source;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false", "sa", "");) {
            Statement stmt = connection.createStatement();
            stmt.addBatch(initDDL);
            stmt.executeBatch();
            stmt.clearBatch();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
