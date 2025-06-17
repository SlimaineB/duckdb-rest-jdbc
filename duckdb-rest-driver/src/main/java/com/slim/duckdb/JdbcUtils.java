package com.slim.duckdb;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

import static com.slim.duckdb.DuckDBDriver.DUCKDB_URL_PREFIX;
import static com.slim.duckdb.DuckDBDriver.MEMORY_DB;

final class JdbcUtils {

    private JdbcUtils() {
    }

    @SuppressWarnings("unchecked")
    static <T> T unwrap(Object obj, Class<T> iface) throws SQLException {
        if (!iface.isInstance(obj)) {
            throw new SQLException(obj.getClass().getName() + " not unwrappable from " + iface.getName());
        }
        return (T) obj;
    }

    static String removeOption(Properties props, String opt) {
        return removeOption(props, opt, null);
    }

    static String removeOption(Properties props, String opt, String defaultVal) {
        Object obj = props.remove(opt);
        if (null != obj) {
            return obj.toString().trim();
        }
        return defaultVal;
    }

    static void setDefaultOptionValue(Properties props, String opt, Object value) {
        if (props.contains(opt)) {
            return;
        }
        props.put(opt, value);
    }

    static boolean isStringTruish(String val, boolean defaultVal) throws SQLException {
        if (null == val) {
            return defaultVal;
        }
        String valLower = val.toLowerCase().trim();
        if (valLower.equals("true") || valLower.equals("1") || valLower.equals("yes") || valLower.equals("on")) {
            return true;
        }
        if (valLower.equals("false") || valLower.equals("0") || valLower.equals("no") || valLower.equals("off")) {
            return false;
        }
        throw new SQLException("Invalid boolean option value: " + val);
    }

    static String dbNameFromUrl(String url) throws SQLException {

        System.out.println("[JdbcUtils] dbNameFromUrl called with URL: " + url);
        if (null == url) {
            throw new SQLException("Invalid null URL specified");
        }
        if (!url.startsWith(DUCKDB_URL_PREFIX)) {
            throw new SQLException("DuckDB JDBC URL needs to start with 'jdbc:duckdb:'");
        }
        final String shortUrl;
        if (url.contains(";")) {
            String[] parts = url.split(";");
            shortUrl = parts[0].trim();
        } else {
            shortUrl = url;
        }
        String dbName = shortUrl.substring(DUCKDB_URL_PREFIX.length()).trim();
        if (dbName.length() == 0) {
            dbName = MEMORY_DB;
        }
        if (dbName.startsWith(MEMORY_DB.substring(1))) {
            dbName = ":" + dbName;
        }
        if(dbName.startsWith("//")) {
            dbName = dbName.split(url.contains("?") ? "\\?" : ";")[0];
        }
        System.out.println("[JdbcUtils] Extracted dbName: " + dbName);
        return dbName;
    }

    static String bytesToHex(byte[] bytes) {
        if (null == bytes) {
            return "";
        }
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    static void closeQuietly(AutoCloseable closeable) {
        if (null == closeable) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            // suppress
        }
    }


    interface TypeProcessor {
        void process(Object[] columnData, DuckDBVector vector) throws Exception;
    }

    public static final Map<String, TypeProcessor> TYPE_PROCESSORS = new HashMap<>();

    static {
        TYPE_PROCESSORS.put("BOOLEAN", new TypeProcessor() {
            public void process(Object[] data, DuckDBVector vec) throws Exception {
                fillPrimitiveBuffer(data.length, vec, new BiConsumer<ByteBuffer, Object[]>() {
                    public void accept(ByteBuffer buffer, Object[] ignored) {
                        for (int i = 0; i < data.length; i++) {
                            buffer.put((byte) (Boolean.TRUE.equals(data[i]) ? 1 : 0));
                        }
                    }
                });
            }
        });

        TYPE_PROCESSORS.put("TINYINT", new TypeProcessor() {
            public void process(Object[] data, DuckDBVector vec) throws Exception {
                fillPrimitiveBuffer(data.length, vec, new BiConsumer<ByteBuffer, Object[]>() {
                    public void accept(ByteBuffer buffer, Object[] ignored) {
                        for (int i = 0; i < data.length; i++) {
                            buffer.put(data[i] == null ? 0 : ((Number) data[i]).byteValue());
                        }
                    }
                });
            }
        });

        TYPE_PROCESSORS.put("UTINYINT", TYPE_PROCESSORS.get("TINYINT"));

        TYPE_PROCESSORS.put("SMALLINT", new TypeProcessor() {
            public void process(Object[] data, DuckDBVector vec) throws Exception {
                fillPrimitiveBuffer(data.length * Short.BYTES, vec, new BiConsumer<ByteBuffer, Object[]>() {
                    public void accept(ByteBuffer buffer, Object[] ignored) {
                        for (int i = 0; i < data.length; i++) {
                            buffer.putShort(data[i] == null ? 0 : ((Number) data[i]).shortValue());
                        }
                    }
                });
            }
        });

        TYPE_PROCESSORS.put("USMALLINT", TYPE_PROCESSORS.get("SMALLINT"));

        TYPE_PROCESSORS.put("INTEGER", new TypeProcessor() {
            public void process(Object[] data, DuckDBVector vec) throws Exception {
                fillPrimitiveBuffer(data.length * Integer.BYTES, vec, new BiConsumer<ByteBuffer, Object[]>() {
                    public void accept(ByteBuffer buffer, Object[] ignored) {
                        for (int i = 0; i < data.length; i++) {
                            buffer.putInt(data[i] == null ? 0 : ((Number) data[i]).intValue());
                        }
                    }
                });
            }
        });

        TYPE_PROCESSORS.put("UINTEGER", TYPE_PROCESSORS.get("INTEGER"));

        TYPE_PROCESSORS.put("BIGINT", new TypeProcessor() {
            public void process(Object[] data, DuckDBVector vec) throws Exception {
                fillPrimitiveBuffer(data.length * Long.BYTES, vec, new BiConsumer<ByteBuffer, Object[]>() {
                    public void accept(ByteBuffer buffer, Object[] ignored) {
                        for (int i = 0; i < data.length; i++) {
                            buffer.putLong(data[i] == null ? 0L : ((Number) data[i]).longValue());
                        }
                    }
                });
            }
        });

        TYPE_PROCESSORS.put("UBIGINT", TYPE_PROCESSORS.get("BIGINT"));

        TYPE_PROCESSORS.put("FLOAT", new TypeProcessor() {
            public void process(Object[] data, DuckDBVector vec) throws Exception {
                fillPrimitiveBuffer(data.length * Float.BYTES, vec, new BiConsumer<ByteBuffer, Object[]>() {
                    public void accept(ByteBuffer buffer, Object[] ignored) {
                        for (int i = 0; i < data.length; i++) {
                            float v = 0f;
                            if (data[i] instanceof Float) {
                                v = ((Float) data[i]).isNaN() ? Float.NaN : ((Float) data[i]);
                            } else if (data[i] instanceof Number) {
                                v = ((Number) data[i]).floatValue();
                                if (Float.isNaN(v)) v = Float.NaN;
                            }
                            buffer.putFloat(data[i] == null ? 0f : v);
                        }
                    }
                });
            }
        });

        TYPE_PROCESSORS.put("DOUBLE", new TypeProcessor() {
            public void process(Object[] data, DuckDBVector vec) throws Exception {
                fillPrimitiveBuffer(data.length * Double.BYTES, vec, new BiConsumer<ByteBuffer, Object[]>() {
                    public void accept(ByteBuffer buffer, Object[] ignored) {
                        for (int i = 0; i < data.length; i++) {
                            double v = 0d;
                            if (data[i] instanceof Double) {
                                v = ((Double) data[i]).isNaN() ? Double.NaN : ((Double) data[i]);
                            } else if (data[i] instanceof Number) {
                                v = ((Number) data[i]).doubleValue();
                                if (Double.isNaN(v)) v = Double.NaN;
                            }
                            buffer.putDouble(data[i] == null ? 0d : v);
                        }
                    }
                });
            }
        });

        TYPE_PROCESSORS.put("VARCHAR", new TypeProcessor() {
            public void process(Object[] data, DuckDBVector vec) throws Exception {
                setVarlenData(data, vec);
            }
        });
        TYPE_PROCESSORS.put("STRING", TYPE_PROCESSORS.get("VARCHAR"));

        TYPE_PROCESSORS.put("TIMESTAMP", new TypeProcessor() {
            public void process(Object[] data, DuckDBVector vec) throws Exception {
                handleTimestamp(data, vec);
            }
        });
        TYPE_PROCESSORS.put("TIMESTAMP_MS", TYPE_PROCESSORS.get("TIMESTAMP"));
        TYPE_PROCESSORS.put("TIMESTAMP_NS", TYPE_PROCESSORS.get("TIMESTAMP"));
        TYPE_PROCESSORS.put("TIMESTAMP_S", TYPE_PROCESSORS.get("TIMESTAMP"));

        TYPE_PROCESSORS.put("HUGEINT", new TypeProcessor() {
            public void process(Object[] data, DuckDBVector vec) throws Exception {
                handleHugeInt(data, vec);
            }
        });
    }

    // Remplacez le switch expression par if/else pour Java 8
    private static void handleHugeInt(Object[] data, DuckDBVector vector) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(data.length * 16).order(ByteOrder.nativeOrder());
        for (int i = 0; i < data.length; i++) {
            BigDecimal hugeInt;
            if (data[i] == null) {
                hugeInt = BigDecimal.ZERO;
            } else if (data[i] instanceof BigDecimal) {
                hugeInt = (BigDecimal) data[i];
            } else if (data[i] instanceof Number) {
                hugeInt = BigDecimal.valueOf(((Number) data[i]).longValue());
            } else {
                throw new SQLException("Type inattendu pour HUGEINT : " + data[i].getClass());
            }
            long lowBits = hugeInt.remainder(BigDecimal.valueOf(1L << 64)).longValue();
            long highBits = hugeInt.divideToIntegralValue(BigDecimal.valueOf(1L << 64)).longValue();
            buffer.putLong(highBits);
            buffer.putLong(lowBits);
        }
        buffer.flip();
        Field f = DuckDBVector.class.getDeclaredField("constlen_data");
        f.setAccessible(true);
        f.set(vector, buffer);
    }    

 private static void fillPrimitiveBuffer(int byteCount, DuckDBVector vector, BiConsumer<ByteBuffer, Object[]> filler) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(byteCount).order(ByteOrder.nativeOrder());
        filler.accept(buffer, null); // Ignore data since it's captured in closure
        buffer.flip();
        Field f = DuckDBVector.class.getDeclaredField("constlen_data");
        f.setAccessible(true);
        f.set(vector, buffer);
    }

    private static void setVarlenData(Object[] data, DuckDBVector vector) throws Exception {
        Field f = DuckDBVector.class.getDeclaredField("varlen_data");
        f.setAccessible(true);
        f.set(vector, data);
    }

    private static void handleTimestamp(Object[] data, DuckDBVector vector) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(data.length * Long.BYTES).order(ByteOrder.nativeOrder());
        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                buffer.putLong(0L);
            } else if (data[i] instanceof Number) {
                buffer.putLong(((Number) data[i]).longValue());
            } else if (data[i] instanceof String ) {
                long millis;
                String s = (String)data[i];
                if (s.contains("T")) {
                    millis = java.time.OffsetDateTime.parse(s).toInstant().toEpochMilli();
                } else {
                    millis = java.sql.Timestamp.valueOf(s).getTime();
                }
                buffer.putLong(millis);
            } else {
                throw new SQLException("Type TIMESTAMP inattendu: " + data[i].getClass());
            }
        }
        buffer.flip();
        Field f = DuckDBVector.class.getDeclaredField("constlen_data");
        f.setAccessible(true);
        f.set(vector, buffer);
    }


}
