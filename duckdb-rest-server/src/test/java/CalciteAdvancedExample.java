import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalciteAdvancedExample {
    public static void main(String[] args) {
        try {
            // Étape 1 : Définir un schéma par défaut
            SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            rootSchema.add("employees", new MockTable(new String[]{"name", "department_id", "salary"}));
            rootSchema.add("departments", new MockTable(new String[]{"id", "name"}));

            // Étape 2 : Configurer le parser SQL avec le schéma
            FrameworkConfig config = Frameworks.newConfigBuilder()
                    .parserConfig(SqlParser.config()
                            .withLex(Lex.MYSQL) // Syntaxe SQL (MySQL dans cet exemple)
                            .withConformance(SqlConformanceEnum.DEFAULT))
                    .defaultSchema(rootSchema) // Définir le schéma par défaut
                    .build();

            // Étape 3 : Créer un planner pour analyser et optimiser
            Planner planner = Frameworks.getPlanner(config);

            // Requête SQL complexe
            String sql = "SELECT e.name, d.name AS department, COUNT(*) AS employee_count " +
                         "FROM employees e " +
                         "JOIN departments d ON e.department_id = d.id " +
                         "WHERE e.salary > 50000 " +
                         "GROUP BY e.name, d.name " +
                         "ORDER BY employee_count DESC";

            // Étape 4 : Analyser la requête
            SqlNode parsedQuery = planner.parse(sql);
            System.out.println("Requête analysée :\n" + parsedQuery.toString());

            // Étape 5 : Valider la requête
            SqlNode validatedQuery = planner.validate(parsedQuery);
            System.out.println("Requête validée :\n" + validatedQuery.toString());

            // Étape 6 : Convertir en arbre relationnel (RelNode)
            RelNode relNode = planner.rel(validatedQuery).project();
            System.out.println("Arbre relationnel (RelNode) :\n" + RelOptUtil.toString(relNode));

            // Étape 7 : Extraire des informations
            System.out.println("Colonnes dans le résultat :");
            List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
            for (RelDataTypeField field : fields) {
                System.out.println(" - " + field.getName() + " (" + field.getType() + ")");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Classe fictive pour simuler une table avec des colonnes
    static class MockTable extends AbstractTable {
        private final String[] columnNames;

        public MockTable(String[] columnNames) {
            this.columnNames = columnNames;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (String columnName : columnNames) {
                builder.add(columnName, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
            }
            return builder.build();
        }
    }
}