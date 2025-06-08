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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.slim.driver.DuckDBHttpClient;
import com.slim.dto.ExecuteResponse;


class DuckDBNative {

    // We use zero-length ByteBuffer-s as a hacky but cheap way to pass C++ pointers
    // back and forth

    /*
     * NB: if you change anything below, run `javah` on this class to re-generate
     * the C header. CMake does this as well
     */

    // results ConnectionHolder reference objectimport java.nio.ByteBuffer;



    private static final Map<ByteBuffer, String> statementMap = new HashMap<>();
    private static final Map<ByteBuffer, DuckDBResultSetMetaData> resultMetaMap = new HashMap<>();
    private static Map<ByteBuffer, List<List<Object>>> resultDataMap = new ConcurrentHashMap<>();
    private static Set<ByteBuffer> alreadyFetchedResults = Collections.newSetFromMap(new IdentityHashMap<>());


    private static final ObjectMapper mapper = new ObjectMapper();

    static ByteBuffer duckdb_jdbc_startup(byte[] path, boolean read_only, Properties props) throws SQLException {
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

        // Enregistre le statement (optionnel : tu peux y mettre aussi la requête ou conn_ref pour vérification)
        statementMap.put(stmtRef, new String(query));

        return stmtRef;
    }
    static void duckdb_jdbc_release(ByteBuffer stmt_ref) {}

    static DuckDBResultSetMetaData duckdb_jdbc_query_result_meta(ByteBuffer result_ref) throws SQLException {
        DuckDBResultSetMetaData meta = resultMetaMap.get(result_ref);
        if (meta == null) {
            throw new SQLException("Aucune métadonnée pour ce result_ref");
        }
        return meta;
    }


    static DuckDBResultSetMetaData duckdb_jdbc_prepared_statement_meta(ByteBuffer stmt_ref) throws SQLException { return null; }

    // returns res_ref result reference object


    static ByteBuffer duckdb_jdbc_execute(ByteBuffer stmt_ref, Object[] params) throws SQLException {
        try {

            
            String statement = statementMap.get(stmt_ref);
            if (statement == null) throw new SQLException("Statement not found for ref");

   
            DuckDBHttpClient client = new DuckDBHttpClient();
            ExecuteResponse response =   client.execute(statement, params);

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




    static void duckdb_jdbc_free_result(ByteBuffer res_ref) {}

    static DuckDBVector[] duckdb_jdbc_fetch(ByteBuffer res_ref, ByteBuffer conn_ref) throws SQLException {
        // Empêche le re-fetch multiple
        if (alreadyFetchedResults.contains(res_ref)) {
            // Retourne un tableau vide => signal que c’est terminé
            return new DuckDBVector[0];
        }
        
        List<List<Object>> rows = resultDataMap.get(res_ref);
        DuckDBResultSetMetaData meta = resultMetaMap.get(res_ref);

        if (rows == null || meta == null) {
            throw new SQLException("Résultat non trouvé pour ce ref");
        }
        alreadyFetchedResults.add(res_ref); // Marque ce résultat comme "lu"

        int columnCount = meta.getColumnCount();
        int rowCount = rows.size();

        DuckDBVector[] vectors = new DuckDBVector[columnCount];

        for (int col = 0; col < columnCount; col++) {
            System.out.println("Traitement de la colonne " + col + " avec " + rowCount + " lignes");
            String duckdbTypeName = meta.getColumnTypeName(col + 1);
            System.out.println("Type de la colonne " + col + ": " + duckdbTypeName);
            Object[] columnData = new Object[rowCount];
            boolean[] nullmask = new boolean[rowCount];

            for (int row = 0; row < rowCount; row++) {
                Object value = rows.get(row).get(col);
                columnData[row] = value;
                nullmask[row] = (value == null);
            }

            DuckDBVector vector = new DuckDBVector(duckdbTypeName, rowCount, nullmask);

            if (isVarlenType(duckdbTypeName)) {
                try {
                    Field f = DuckDBVector.class.getDeclaredField("varlen_data");
                    f.setAccessible(true);
                    f.set(vector, columnData);
                } catch (Exception e) {
                    throw new SQLException("Erreur lors du remplissage de varlen_data", e);
                }
            } else {
                try {
                    ByteBuffer buffer;
                    switch (duckdbTypeName.toUpperCase()) {
                        case "INTEGER":
                            buffer = ByteBuffer.allocate(rowCount * Integer.BYTES).order(ByteOrder.nativeOrder());
                            for (int i = 0; i < rowCount; i++) {
                                buffer.putInt(columnData[i] == null ? 0 : ((Number) columnData[i]).intValue());
                            }
                            break;
                        case "BIGINT":
                            buffer = ByteBuffer.allocate(rowCount * Long.BYTES).order(ByteOrder.nativeOrder());
                            for (int i = 0; i < rowCount; i++) {
                                buffer.putLong(columnData[i] == null ? 0L : ((Number) columnData[i]).longValue());
                            }
                            break;
                        case "FLOAT":
                            buffer = ByteBuffer.allocate(rowCount * Float.BYTES).order(ByteOrder.nativeOrder());
                            for (int i = 0; i < rowCount; i++) {
                                buffer.putFloat(columnData[i] == null ? 0.0f : ((Number) columnData[i]).floatValue());
                            }
                            break;
                        case "DOUBLE":
                            buffer = ByteBuffer.allocate(rowCount * Double.BYTES).order(ByteOrder.nativeOrder());
                            for (int i = 0; i < rowCount; i++) {
                                buffer.putDouble(columnData[i] == null ? 0.0d : ((Number) columnData[i]).doubleValue());
                            }
                            break;
                        case "BOOLEAN":
                            buffer = ByteBuffer.allocate(rowCount).order(ByteOrder.nativeOrder());
                            for (int i = 0; i < rowCount; i++) {
                                buffer.put((byte) ((Boolean.TRUE.equals(columnData[i])) ? 1 : 0));
                            }
                            break;
                        default:
                            throw new SQLException("Type non supporté pour constlen_data : " + duckdbTypeName);
                    }

                    buffer.flip();
                    Field f = DuckDBVector.class.getDeclaredField("constlen_data");
                    f.setAccessible(true);
                    f.set(vector, buffer);

                } catch (Exception e) {
                    throw new SQLException("Erreur lors du remplissage de constlen_data", e);
                }
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
