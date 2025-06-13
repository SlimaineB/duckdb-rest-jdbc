import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.config.Lex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class CalcitePartitionSplitExample {
    private static final Logger logger = LoggerFactory.getLogger(CalcitePartitionSplitExample.class);

    public static void main(String[] args) throws Exception {
        // Paramètres d'entrée
        String sql = "SELECT MIN(e.salary) as min_sal, max(e.salary) AS max_salary, COUNT(e.salary) AS nb, AVG(e.salary) AS avg_sal, SUM(e.salary), department_id FROM employees e WHERE e.salary > 1000 GROUP BY department_id ORDER BY department_id";
        //String sql = "SELECT MIN(e.salary) as min_sal, max(e.salary) AS max_salary, COUNT(e.salary) AS nb, AVG(e.salary) AS avg_sal, SUM(e.salary) FROM employees e WHERE e.salary > 1000";
        List<String> partitionColumns = Arrays.asList("department_id");

        logger.info("Début du test de partitionnement Calcite/DuckDB");
        logger.debug("SQL d'origine : {}", sql);
        logger.debug("Colonnes de partition : {}", partitionColumns);

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE employees (name VARCHAR, department_id INTEGER, salary INTEGER)");
            stmt.execute("CREATE TABLE departments (id INTEGER, name VARCHAR)");
            // Données de test : 2 départements, salaires variés
            stmt.execute("INSERT INTO employees VALUES " +
                    "('Alice', 1, 6000), " +   // dep 1
                    "('Bob', 2, 4000), " +     // dep 2
                    "('Charlie', 1, 7000), " + // dep 1
                    "('Diana', 2, 2000), " +   // dep 2
                    "('Eve', 1, 1200), " +     // dep 1
                    "('Frank', 2, 1500), " +   // dep 2
                    "('Grace', 1, 800), " +    // dep 1 (sera filtré par WHERE)
                    "('Heidi', 2, 900)");      // dep 2 (sera filtré par WHERE)
            stmt.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'HR')");

            logger.info("Tables et données de test créées.");

            // 1. Récupérer dynamiquement toutes les combinaisons de partitions
            String partitionQuery = "SELECT DISTINCT " + String.join(", ", partitionColumns) + " FROM employees";
            logger.debug("Partition query : {}", partitionQuery);
            List<Map<String, Object>> partitions = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(partitionQuery)) {
                while (rs.next()) {
                    Map<String, Object> part = new LinkedHashMap<>();
                    for (String col : partitionColumns) {
                        part.put(col, rs.getObject(col));
                    }
                    partitions.add(part);
                }
            }
            logger.info("Partitions détectées : {}", partitions);

            // 2. Générer et exécuter les requêtes partitionnées
            // Avant le parsing :
            sql = ensureSumForAvg(sql);
            logger.debug("SQL après ensureSumForAvg : {}", sql);
            SqlParser parser = SqlParser.create(sql, SqlParser.config().withLex(Lex.MYSQL));
            SqlNode node = parser.parseQuery();
            SqlSelect select = (node instanceof SqlOrderBy) ? (SqlSelect) ((SqlOrderBy) node).query : (SqlSelect) node;

            List<String> nonSplit = getNonSplittableAggregations(select);
            if (!nonSplit.isEmpty()) {
                logger.warn("Split désactivé : fonctions d'agrégation non splitables détectées : {}", nonSplit);
                // Exécution directe
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        Map<String, Object> globalAgg = new LinkedHashMap<>();
                        ResultSetMetaData meta = rs.getMetaData();
                        for (int i = 1; i <= meta.getColumnCount(); i++) {
                            globalAgg.put(meta.getColumnLabel(i), rs.getObject(i));
                        }
                        logger.info("Agrégation globale (sans split) : {}", globalAgg);
                    }
                }
                return;
            }

            // Pour chaque colonne d'agrégation, stocker la liste des résultats partiels
            List<Map<String, Object>> partialResults = new ArrayList<>();
            List<String> aggColumns = new ArrayList<>();

            for (Map<String, Object> part : partitions) {
                SqlNode partitioned = addPartitionFilterGeneric(select, partitionColumns, part);
                String sqlPartition = partitioned.toSqlString(AnsiSqlDialect.DEFAULT).getSql();
                // Nettoyage pour DuckDB
                sqlPartition = sqlPartition.replace("`", "");
                logger.debug("SQL partitionné : {}", sqlPartition);
                try (ResultSet rs = stmt.executeQuery(sqlPartition)) {
                    ResultSetMetaData meta = rs.getMetaData();
                    if (aggColumns.isEmpty()) {
                        for (int i = 1; i <= meta.getColumnCount(); i++) {
                            aggColumns.add(meta.getColumnLabel(i));
                        }
                    }
                    while (rs.next()) {
                        Map<String, Object> result = new HashMap<>();
                        for (String col : aggColumns) {
                            result.put(col, rs.getObject(col));
                        }
                        partialResults.add(result);
                        logger.info("Partiel: {} pour partition {}", result, part);
                    }
                }
            }

            // Agrégation finale
            Map<String, Object> finalAgg = new HashMap<>();
            Long totalSum = null;
            Long totalCount = null;
            for (String col : aggColumns) {
                Object agg = null;
                for (Map<String, Object> part : partialResults) {
                    Object val = part.get(col);
                    if (val == null) continue;
                    if (val instanceof Number) {
                        Number n = (Number) val;
                        if (col.toLowerCase().contains("sum") || col.toLowerCase().contains("total")) {
                            agg = (agg == null) ? n.longValue() : ((Long) agg + n.longValue());
                            totalSum = (totalSum == null) ? n.longValue() : (totalSum + n.longValue());
                        } else if (col.toLowerCase().contains("count") || col.toLowerCase().contains("nb")) {
                            agg = (agg == null) ? n.longValue() : ((Long) agg + n.longValue());
                            totalCount = (totalCount == null) ? n.longValue() : (totalCount + n.longValue());
                        } else if (col.toLowerCase().contains("min")) {
                            agg = (agg == null) ? n.longValue() : Math.min((Long) agg, n.longValue());
                        } else if (col.toLowerCase().contains("max")) {
                            agg = (agg == null) ? n.longValue() : Math.max((Long) agg, n.longValue());
                        }
                    }
                }
                // On ignore avg ici, il sera calculé après
                if (!col.toLowerCase().contains("avg")) {
                    finalAgg.put(col, agg);
                }
            }
            // Calcul AVG si demandé dans la requête
            for (String col : aggColumns) {
                if (col.toLowerCase().contains("avg")) {
                    // Cherche dynamiquement la colonne SUM correspondante
                    String sumCol = null;
                    for (String c : aggColumns) {
                        if (c.toLowerCase().startsWith("sum(") || c.toLowerCase().startsWith("sum_") || c.toLowerCase().equals("sum(e.salary)") || c.toLowerCase().equals("sum(salary)")) {
                            sumCol = c;
                            break;
                        }
                    }
                    Object sumObj = sumCol != null ? finalAgg.get(sumCol) : null;
                    Object countObj = finalAgg.get("nb");
                    if (sumObj instanceof Number && countObj instanceof Number && ((Number) countObj).longValue() != 0) {
                        double avg = ((Number) sumObj).doubleValue() / ((Number) countObj).doubleValue();
                        finalAgg.put(col, avg);
                    } else {
                        finalAgg.put(col, null);
                    }
                }
            }
            logger.info("Agrégation finale : {}", finalAgg);

            // Comparaison avec la requête globale (sans split)
            try (ResultSet rs = stmt.executeQuery(sql.replace("e.salary", "salary").replace("e.", ""))) {
                while (rs.next()) {
                    Map<String, Object> globalAgg = new LinkedHashMap<>();
                    ResultSetMetaData meta = rs.getMetaData();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        globalAgg.put(meta.getColumnLabel(i), rs.getObject(i));
                    }
                    logger.info("Agrégation globale (sans split) : {}", globalAgg);
                }
            }
        }
    }

    // Ajoute dynamiquement un filtre de partition multi-colonnes à la requête
    static SqlNode addPartitionFilterGeneric(SqlNode node, List<String> partitionCols, Map<String, Object> values) {
        if (!(node instanceof SqlSelect)) throw new IllegalArgumentException("Only SELECT supported");
        SqlSelect select = (SqlSelect) node;
        SqlNode where = select.getWhere();

        // Construit la conjonction de filtres pour chaque colonne de partition
        List<SqlNode> filters = new ArrayList<>();
        for (String col : partitionCols) {
            Object val = values.get(col);
            SqlNode valueNode = (val instanceof Number)
                    ? SqlLiteral.createExactNumeric(val.toString(), SqlParserPos.ZERO)
                    : SqlLiteral.createCharString(val.toString(), SqlParserPos.ZERO);
            filters.add(new SqlBasicCall(
                    SqlStdOperatorTable.EQUALS,
                    new SqlNode[]{
                            new SqlIdentifier("e." + col, SqlParserPos.ZERO),
                            valueNode
                    },
                    SqlParserPos.ZERO
            ));
        }
        SqlNode partitionFilter = filters.size() == 1 ? filters.get(0)
                : new SqlBasicCall(SqlStdOperatorTable.AND, filters.toArray(new SqlNode[0]), SqlParserPos.ZERO);

        SqlNode newWhere = (where == null)
                ? partitionFilter
                : new SqlBasicCall(SqlStdOperatorTable.AND, new SqlNode[]{where, partitionFilter}, SqlParserPos.ZERO);

        return new SqlSelect(
                select.getParserPosition(),
                SqlNodeList.EMPTY, // keywordList
                select.getSelectList(),
                select.getFrom(),
                newWhere,
                select.getGroup(),
                select.getHaving(),
                select.getWindowList(),
                null, // qualify
                select.getOrderList(),
                select.getOffset(),
                select.getFetch(),
                null // hints
        );
    }

    static boolean isSplittableAggregation(SqlNode node) {
        if (!(node instanceof SqlSelect)) return false;
        SqlSelect select = (SqlSelect) node;
        SqlNodeList selectList = select.getSelectList();
        // Liste blanche des fonctions splitables
        List<String> allowed = Arrays.asList("sum", "count", "min", "max", "avg");
        for (SqlNode item : selectList) {
            String expr = item.toString().toLowerCase();
            boolean found = false;
            for (String fn : allowed) {
                if (expr.contains(fn + "(")) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

    static List<String> getNonSplittableAggregations(SqlSelect select) {
        SqlNodeList selectList = select.getSelectList();
        SqlNodeList groupBy = select.getGroup();
        Set<String> groupByCols = new HashSet<>();
        if (groupBy != null) {
            for (SqlNode groupExpr : groupBy) {
                groupByCols.add(groupExpr.toString().toLowerCase());
            }
        }
        List<String> allowed = Arrays.asList("sum", "count", "min", "max", "avg");
        List<String> nonSplit = new ArrayList<>();
        for (SqlNode item : selectList) {
            String expr = item.toString().toLowerCase();
            boolean isGroupByCol = groupByCols.contains(expr);
            boolean found = false;
            for (String fn : allowed) {
                if (expr.contains(fn + "(")) {
                    found = true;
                    break;
                }
            }
            // Si ce n'est pas une agrégation autorisée ET pas une colonne du group by, on l'ajoute
            if (!found && !isGroupByCol) nonSplit.add(expr);
        }
        return nonSplit;
    }

    static String ensureSumForAvg(String sql) throws Exception {
        SqlParser parser = SqlParser.create(sql, SqlParser.config().withLex(Lex.MYSQL));
        SqlNode node = parser.parseQuery();
        if (!(node instanceof SqlSelect)) return sql;
        SqlSelect select = (SqlSelect) node;
        SqlNodeList selectList = select.getSelectList();
        List<SqlNode> newSelectList = new ArrayList<>();
        Set<String> alreadyPresent = new HashSet<>();
        // Collecte les expressions déjà présentes
        for (SqlNode item : selectList) {
            alreadyPresent.add(item.toString().toLowerCase());
        }
        // Ajoute SUM pour chaque AVG manquant
        for (SqlNode item : selectList) {
            if (item instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) item;
                if (call.getOperator().getName().equalsIgnoreCase("AVG")) {
                    SqlNode arg = call.getOperandList().get(0);
                    String alias = null;
                    if (call.getOperandList().size() > 1 && call.getOperandList().get(1) instanceof SqlIdentifier) {
                        alias = ((SqlIdentifier) call.getOperandList().get(1)).getSimple();
                    }
                    String sumAlias = "sum_" + (alias != null ? alias : arg.toString().replace(".", "_"));
                    String sumExpr = "SUM(" + arg.toString() + ") AS " + sumAlias;
                    if (!alreadyPresent.contains(sumExpr.toLowerCase())) {
                        // Ajoute la colonne SUM correspondante
                        SqlNode sumNode = SqlStdOperatorTable.SUM.createCall(SqlParserPos.ZERO, arg);
                        SqlNode sumAliasNode = new SqlBasicCall(
                            SqlStdOperatorTable.AS,
                            new SqlNode[]{sumNode, new SqlIdentifier(sumAlias, SqlParserPos.ZERO)},
                            SqlParserPos.ZERO
                        );
                        newSelectList.add(sumAliasNode);
                    }
                }
            }
        }
        // Ajoute tous les select existants
        for (SqlNode item : selectList) {
            newSelectList.add(item);
        }
        // Si rien à ajouter, retourne la requête d'origine
        if (newSelectList.size() == selectList.size()) return sql;
        // Reconstruit la requête
        SqlSelect newSelect = new SqlSelect(
            select.getParserPosition(),
            SqlNodeList.EMPTY,
            new SqlNodeList(newSelectList, SqlParserPos.ZERO),
            select.getFrom(),
            select.getWhere(),
            select.getGroup(),
            select.getHaving(),
            select.getWindowList(),
            select.getQualify(),
            select.getOrderList(),
            select.getOffset(),
            select.getFetch(),
            select.getHints()
        );

        return newSelect.toSqlString(AnsiSqlDialect.DEFAULT).getSql();
    }
}
