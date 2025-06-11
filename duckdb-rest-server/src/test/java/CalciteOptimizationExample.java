import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.util.List;

public class CalciteOptimizationExample {
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

            // Requête SQL non optimale
            String sql = "SELECT e.name, d.name AS department " +
                         "FROM employees e " +
                         "JOIN departments d ON e.department_id = d.id " +
                         "WHERE e.salary > 50000 AND d.name = 'Engineering'";

            // Étape 4 : Analyser la requête
            SqlNode parsedQuery = planner.parse(sql);
            System.out.println("Requête analysée :\n" + parsedQuery.toString());

            // Étape 5 : Valider la requête
            SqlNode validatedQuery = planner.validate(parsedQuery);
            System.out.println("Requête validée :\n" + validatedQuery.toString());

            // Étape 6 : Convertir en arbre relationnel (RelNode)
            RelNode relNode = planner.rel(validatedQuery).project();
            System.out.println("Arbre relationnel avant optimisation :\n" + relNode);

            // Étape 7 : Appliquer des règles d'optimisation
            RelOptPlanner relOptPlanner = relNode.getCluster().getPlanner(); // Utiliser le même planner
            relOptPlanner.addRule(CoreRules.FILTER_INTO_JOIN); // Pousse les filtres dans les jointures
            relOptPlanner.addRule(CoreRules.PROJECT_MERGE);    // Fusionne les projections inutiles
            relOptPlanner.addRule(CoreRules.PROJECT_REMOVE);   // Supprime les projections inutiles
            // Règle de conversion vers Enumerable (indispensable)
            relOptPlanner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
            relOptPlanner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
            relOptPlanner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
            relOptPlanner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);

            // Appliquer les règles
            relOptPlanner.setRoot(relNode);
            RelNode optimizedRelNode = relOptPlanner.findBestExp();

            System.out.println("Arbre relationnel après optimisation :\n" + optimizedRelNode);

            // Étape 8 : Extraire des informations
            System.out.println("Colonnes dans le résultat :");
            List<RelDataTypeField> fields = optimizedRelNode.getRowType().getFieldList();
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
                switch (columnName) {
                    case "salary":
                    case "department_id":
                    case "id":
                        builder.add(columnName, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER));
                        break;
                    default:
                        builder.add(columnName, typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
                }
            }
            return builder.build();
        }
    }
}