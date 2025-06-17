package com.slim.duckdb;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.slim.dto.ExecuteResponse;
import com.slim.duckdb.JdbcUtils.TypeProcessor;
import com.slim.duckdb.client.DuckDBHttpClient;
import static com.slim.duckdb.JdbcUtils.TYPE_PROCESSORS;


class DuckDBNative {

    // We use zero-length ByteBuffer-s as a hacky but cheap way to pass C++ pointers
    // back and forth

    /*
     * NB: if you change anything below, run `javah` on this class to re-generate
     * the C header. CMake does this as well
     */

    // results ConnectionHolder reference objectimport java.nio.ByteBuffer;

    private static String backendUrl = "http://localhost:8080"; // Default URL for DuckDB REST server
    private static final Map<ByteBuffer, DuckDBConnection> connectionMap = new HashMap<>();
    private static final Map<ByteBuffer, String> statementMap = new HashMap<>();
    private static final Map<ByteBuffer, Object[]> statementParamsMap = new HashMap<>();
    private static final Map<ByteBuffer, DuckDBResultSetMetaData> resultMetaMap = new HashMap<>();
    private static Map<ByteBuffer, List<List<Object>>> resultDataMap = new ConcurrentHashMap<>();
    private static Set<ByteBuffer> alreadyFetchedResults = Collections.newSetFromMap(new IdentityHashMap<>());



    static ByteBuffer duckdb_jdbc_startup(byte[] path, boolean read_only, Properties props) throws SQLException {


        backendUrl = JdbcUtils.removeOption(props, "backendUrl", backendUrl);
        if (backendUrl == null || backendUrl.isEmpty()) {
            throw new SQLException("Backend URL must be specified in properties");
        }
        
        boolean useEncryption= JdbcUtils.isStringTruish(JdbcUtils.removeOption(props, "useEncryption", "false"), false);
        if(useEncryption) {
            backendUrl = "https://" + backendUrl; 
        }else{
            backendUrl = "http://" + backendUrl; 
        }

        System.out.println("[DuckDBNative] Backend URL set to: " + backendUrl);
        // Crée un UUID unique
        UUID uuid = UUID.randomUUID();

        // Place-le dans un ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());

        // Remets le pointeur à zéro pour une lecture future
        buffer.flip();
        return buffer;
    }


    // returns conn_ref connection reference object
    static ByteBuffer duckdb_jdbc_connect(ByteBuffer conn_ref) throws SQLException { return null; }

    static ByteBuffer duckdb_jdbc_create_db_ref(ByteBuffer conn_ref) throws SQLException { return null; }

    static void duckdb_jdbc_destroy_db_ref(ByteBuffer db_ref) throws SQLException {}

    static void duckdb_jdbc_set_auto_commit(ByteBuffer conn_ref, boolean auto_commit) throws SQLException {}


    static void duckdb_jdbc_disconnect(ByteBuffer conn_ref) {}

    static void duckdb_jdbc_set_schema(ByteBuffer conn_ref, String schema) {}

    static void duckdb_jdbc_set_catalog(ByteBuffer conn_ref, String catalog) {}

    static String duckdb_jdbc_get_schema(ByteBuffer conn_ref) { return null; }

    static String duckdb_jdbc_get_catalog(ByteBuffer conn_ref) { return null; }

    static ByteBuffer duckdb_jdbc_prepare(ByteBuffer conn_ref, byte[] query) throws SQLException {
        

        // Simule la création d'un statement préparé
        UUID stmtId = UUID.randomUUID();

        ByteBuffer stmtRef = ByteBuffer.allocate(16);
        stmtRef.putLong(stmtId.getMostSignificantBits());
        stmtRef.putLong(stmtId.getLeastSignificantBits());
        stmtRef.flip();

        
        statementMap.put(stmtRef, new String(query));


        return stmtRef;
    }
    static void duckdb_jdbc_release(ByteBuffer stmt_ref) {
        if (stmt_ref == null) {
            System.err.println("Statement reference is null. Nothing to release.");
            return;
        }

        // Supprime le statement de la map des statements
        String removedStatement = statementMap.remove(stmt_ref);
        if (removedStatement != null) {
            System.out.println("Statement released: " + removedStatement);
        } else {
            System.err.println("No statement found for the provided reference.");
        }

        // Supprime les paramètres associés au statement
        Object[] removedParams = statementParamsMap.remove(stmt_ref);
        if (removedParams != null) {
            System.out.println("Parameters associated with the statement have been released.");
        }

        // Libère la mémoire associée au ByteBuffer
        stmt_ref.clear();
    }

    static DuckDBResultSetMetaData duckdb_jdbc_query_result_meta(ByteBuffer result_ref) throws SQLException {
        DuckDBResultSetMetaData meta = resultMetaMap.get(result_ref);
        if (meta == null) {
            throw new SQLException("Aucune métadonnée pour ce result_ref");
        }
        return meta;
    }


    static DuckDBResultSetMetaData duckdb_jdbc_prepared_statement_meta(ByteBuffer stmt_ref, Object[] params) throws SQLException {
        try {
            String statement = statementMap.get(stmt_ref);
            System.out.println("Paramètres reçus pour la récupération des métadonnées : " + (params != null ? params.length : 0));

            System.out.println("Récupération des métadonnées pour le statement : " + statement);    

            if (statement == null) throw new SQLException("Statement not found for ref");

            DuckDBHttpClient client = new DuckDBHttpClient(backendUrl);

            // On envoie une exécution à vide juste pour récupérer les métadonnées
            ExecuteResponse response = client.execute(statement, params);
            if(response.isError()) {
                throw new SQLException(response.getErrorMessage());
            }

            return response.getMetadata().toDuckDBResultSetMetaData(); 

        } catch (Exception e) {
            throw new SQLException("Erreur HTTP dans duckdb_jdbc_prepared_statement_meta", e);
        }
    }



    // returns res_ref result reference object


    static ByteBuffer duckdb_jdbc_execute(ByteBuffer stmt_ref, Object[] params) throws SQLException {
        try {

            System.out.println("Paramètres reçus pour l'exécution : " + (params != null ? params.length : 0));
            String statement = statementMap.get(stmt_ref);
            if (statement == null) throw new SQLException("Statement not found for ref");

   
            DuckDBHttpClient client = new DuckDBHttpClient(backendUrl);
            ExecuteResponse response =   client.execute(statement, params);
            if(response.isError()) {
                throw new SQLException(response.getErrorMessage());
            }

            System.out.println("Exécution réussie pour le statement : " + statement);
            
            System.out.println("Response reçue : " + response.getData().size() + " lignes, " + response.getMetadata().getColumn_count() + " colonnes");

            // Simuler un result_ref avec UUID
            UUID resultId = UUID.randomUUID();
            ByteBuffer resultRef = ByteBuffer.allocate(16);
            resultRef.putLong(resultId.getMostSignificantBits());
            resultRef.putLong(resultId.getLeastSignificantBits());
            resultRef.flip();

            resultDataMap.put(resultRef, response.getData());   

            resultMetaMap.put(resultRef, response.getMetadata().toDuckDBResultSetMetaData());

            return resultRef;

        } catch (Exception e) {
            throw new SQLException("Erreur HTTP dans execute", e);
        }
    }




    static void duckdb_jdbc_free_result(ByteBuffer res_ref) {
        if (res_ref == null) {
            System.err.println("Result reference is null. Nothing to free.");
            return;
        }

        // Supprime les données associées au résultat
        List<List<Object>> removedData = resultDataMap.remove(res_ref);
        if (removedData != null) {
            System.out.println("Result data has been freed.");
        } else {
            System.err.println("No result data found for the provided reference.");
        }

        // Supprime les métadonnées associées au résultat
        DuckDBResultSetMetaData removedMeta = resultMetaMap.remove(res_ref);
        if (removedMeta != null) {
            System.out.println("Result metadata has been freed.");
        } else {
            System.err.println("No result metadata found for the provided reference.");
        }

        // Supprime le résultat de la liste des résultats déjà lus
        if (alreadyFetchedResults.remove(res_ref)) {
            System.out.println("Result reference removed from fetched results.");
        }

        // Libère la mémoire associée au ByteBuffer
        res_ref.clear();
    }

    public static DuckDBVector[] duckdb_jdbc_fetch(ByteBuffer res_ref, ByteBuffer conn_ref) throws SQLException {
        if (alreadyFetchedResults.contains(res_ref)) {
            return new DuckDBVector[0];
        }

        List<List<Object>> rows = resultDataMap.get(res_ref);
        DuckDBResultSetMetaData meta = resultMetaMap.get(res_ref);

        if (rows == null || meta == null) {
            throw new SQLException("Résultat non trouvé pour ce ref");
        }

        alreadyFetchedResults.add(res_ref);
        int columnCount = meta.getColumnCount();
        int rowCount = rows.size();
        DuckDBVector[] vectors = new DuckDBVector[columnCount];

        for (int col = 0; col < columnCount; col++) {
            String type = meta.getColumnTypeName(col + 1).toUpperCase();
            Object[] columnData = new Object[rowCount];
            boolean[] nullmask = new boolean[rowCount];

            for (int row = 0; row < rowCount; row++) {
                Object value = rows.get(row).get(col);
                columnData[row] = value;
                nullmask[row] = (value == null);
            }

            DuckDBVector vector = new DuckDBVector(type, rowCount, nullmask);
            TypeProcessor processor = TYPE_PROCESSORS.get(type);
            if (processor == null) {
                throw new SQLException("Type non supporté : " + type);
            }

            try {
                processor.process(columnData, vector);
            } catch (Exception e) {
                throw new SQLException("Erreur de traitement pour le type " + type, e);
            }

            vectors[col] = vector;
        }

        return vectors;
    }



    private static boolean isVarlenType(String duckdbType) {
        return duckdbType != null && (
            duckdbType.equalsIgnoreCase("VARCHAR")
            || duckdbType.equalsIgnoreCase("STRING")
            || duckdbType.equalsIgnoreCase("JSON")
            || duckdbType.equalsIgnoreCase("BLOB")
            || duckdbType.equalsIgnoreCase("UUID")
            || duckdbType.equalsIgnoreCase("MAP")
        );
    }




    static int duckdb_jdbc_fetch_size() { return 0; }

    static long duckdb_jdbc_arrow_stream(ByteBuffer res_ref, long batch_size) { return 0; }

    static void duckdb_jdbc_arrow_register(ByteBuffer conn_ref, long arrow_array_stream_pointer, byte[] name) {}

    static ByteBuffer duckdb_jdbc_create_appender(ByteBuffer conn_ref, byte[] schema_name, byte[] table_name) throws SQLException { return null; }

    static void duckdb_jdbc_appender_begin_row(ByteBuffer appender_ref) throws SQLException {}

    static void duckdb_jdbc_appender_end_row(ByteBuffer appender_ref) throws SQLException {}

    static void duckdb_jdbc_appender_flush(ByteBuffer appender_ref) throws SQLException {}

    static void duckdb_jdbc_interrupt(ByteBuffer conn_ref) {}

    static QueryProgress duckdb_jdbc_query_progress(ByteBuffer conn_ref) { return null; }

    static void duckdb_jdbc_appender_close(ByteBuffer appender_ref) throws SQLException {}

    static void duckdb_jdbc_appender_append_boolean(ByteBuffer appender_ref, boolean value) throws SQLException {}

    static void duckdb_jdbc_appender_append_byte(ByteBuffer appender_ref, byte value) throws SQLException {}

    static void duckdb_jdbc_appender_append_short(ByteBuffer appender_ref, short value) throws SQLException {}

    static void duckdb_jdbc_appender_append_int(ByteBuffer appender_ref, int value) throws SQLException {}

    static void duckdb_jdbc_appender_append_long(ByteBuffer appender_ref, long value) throws SQLException {}

    static void duckdb_jdbc_appender_append_float(ByteBuffer appender_ref, float value) throws SQLException {}

    static void duckdb_jdbc_appender_append_double(ByteBuffer appender_ref, double value) throws SQLException {}

    static void duckdb_jdbc_appender_append_string(ByteBuffer appender_ref, byte[] value) throws SQLException {}

    static void duckdb_jdbc_appender_append_bytes(ByteBuffer appender_ref, byte[] value) throws SQLException {}

    static void duckdb_jdbc_appender_append_timestamp(ByteBuffer appender_ref, long value) throws SQLException {}

    static void duckdb_jdbc_appender_append_decimal(ByteBuffer appender_ref, BigDecimal value) throws SQLException {}

    static void duckdb_jdbc_appender_append_null(ByteBuffer appender_ref) throws SQLException {}

    static void duckdb_jdbc_create_extension_type(ByteBuffer conn_ref) throws SQLException {}

    protected static String duckdb_jdbc_get_profiling_information(ByteBuffer conn_ref, ProfilerPrintFormat format) throws SQLException { return null; }

    public static void duckdb_jdbc_create_extension_type(DuckDBConnection conn) throws SQLException {
        duckdb_jdbc_create_extension_type(conn.connRef);
    }
}
