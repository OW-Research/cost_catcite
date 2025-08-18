package org.example;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;

import java.util.Collections;
import java.util.Properties;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.lang.reflect.Method;

public class CalciteCustomStatsExample {

    public static void main(String[] args) throws Exception {
        // 1. Define Schema
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        // Fabricated row counts for our tables
        // We'll tell the optimizer that 'users' is small and 'products' is large.
        rootSchema.add("USERS", new CustomTable(100.0)); // 100 rows
        rootSchema.add("PRODUCTS", new CustomTable(1_000_000.0)); // 1,000,000 rows

        // 2. Create Planner & Tools
        SqlParser.Config parserConfig = SqlParser.config().withCaseSensitive(false);

        // 3. Setup Custom Metadata Provider
        // This is the key part. We chain our custom provider with the default one.
        RelMetadataProvider customProvider = ChainedRelMetadataProvider.of(
                ImmutableList.of(
                        CustomRowCountMetadataProvider.INSTANCE,
                        DefaultRelMetadataProvider.INSTANCE
                )
        );

        // Create a planner and set the custom metadata provider
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

    // Add rules for the optimizer to consider. JoinCommute is a classic rule
    // that swaps the order of joins. The optimizer will use our statistics
    // to decide if swapping is cheaper.
    planner.addRule(CoreRules.JOIN_COMMUTE);
    // Add Enumerable rules so Calcite can produce a physical plan
    planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(new JavaTypeFactoryImpl()));

        // Set the custom metadata provider on the cluster
        cluster.setMetadataProvider(customProvider);


        // 4. Parse and Validate SQL
        String sql = "SELECT u.id, p.name " +
                     "FROM USERS u JOIN PRODUCTS p ON u.id = p.id";

        SqlParser parser = SqlParser.create(sql, parserConfig);
        SqlNode sqlNode = parser.parseQuery();

        // Validator
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                Collections.singletonList(rootSchema.getName()),
                new JavaTypeFactoryImpl(),
                new CalciteConnectionConfigImpl(new Properties())
        );

        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                new JavaTypeFactoryImpl(),
                SqlValidator.Config.DEFAULT
        );
        SqlNode validatedSqlNode = validator.validate(sqlNode);

        // 5. Convert SQL to Relational Algebra (RelNode)
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
                (type, query, schema, path) -> null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config()
        );

        RelRoot relRoot = sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
        RelNode logicalPlan = relRoot.rel;

        System.out.println("---- Initial Logical Plan ----");
        System.out.println(RelOptUtil.toString(logicalPlan));
        // In the initial plan, the join order is based on the FROM clause order.
        // PRODUCTS will likely be on the right (inner) side.

        // 6. Optimize the Plan
    RelTraitSet desiredTraits = logicalPlan.getTraitSet().replace(EnumerableConvention.INSTANCE);
        planner.setRoot(logicalPlan);
        RelNode optimizedPlan = planner.findBestExp();

        System.out.println("\n---- Optimized Plan ----");
        System.out.println(RelOptUtil.toString(optimizedPlan));
        // In the optimized plan, the JoinCommuteRule should have fired.
        // Because USERS (100 rows) is much smaller than PRODUCTS (1M rows),
        // the optimizer will choose a plan where USERS is the left (outer) table
        // in the join, as this is typically more efficient.
    }

    /**
     * A custom table that holds a fabricated row count.
     */
    public static class CustomTable extends AbstractTable {
        private final double rowCount;

        public CustomTable(double rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            builder.add("ID", typeFactory.createJavaType(int.class));
            builder.add("NAME", typeFactory.createJavaType(String.class));
            return builder.build();
        }

        public double getRowCount() {
            return rowCount;
        }
    }

    /**
     * Custom Metadata Provider that supplies the row count for our CustomTable.
     */
    public static class CustomRowCountMetadataProvider implements RelMetadataProvider {
        public static final CustomRowCountMetadataProvider INSTANCE =
                new CustomRowCountMetadataProvider();

        private CustomRowCountMetadataProvider() {}

        @Override
        public <M extends Metadata> UnboundMetadata<M> apply(
                Class<? extends RelNode> relClass,
                Class<? extends M> metadataClass) {
            if (metadataClass == BuiltInMetadata.RowCount.class) {
                return (UnboundMetadata<M>) new RowCountHandler();
            }
            return null;
        }

        @Override
        public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers(MetadataDef<M> def) {
            return ImmutableMultimap.of();
        }

        // Required by RelMetadataProvider in Calcite 1.38.0
        @Override
        public java.util.List<org.apache.calcite.rel.metadata.MetadataHandler<?>> handlers(
                Class<? extends org.apache.calcite.rel.metadata.MetadataHandler<?>> handlerClass) {
            return java.util.Collections.emptyList();
        }

    }


    /**
     * The handler that provides the actual metadata. The method signature now
     * correctly matches the one in the Handler interface.
     */
    public static class RowCountHandler implements BuiltInMetadata.RowCount.Handler {
        @Override
        public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
            if (rel instanceof TableScan) {
                TableScan tableScan = (TableScan) rel;
                // Check if the table scan is for our custom table.
                CustomTable customTable = tableScan.getTable().unwrap(CustomTable.class);
                if (customTable != null) {
                    // If it is, return our fabricated row count.
                    System.out.println("--> Supplying fabricated row count for: " +
                            tableScan.getTable().getQualifiedName() + " (" + customTable.getRowCount() + ")");
                    return customTable.getRowCount();
                }
            }
            // Let the next provider in the chain handle it.
            // Returning null signifies that we don't have the metadata.
            return null;
        }
    }
}
