package org.example;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.config.Lex;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqlToRelConverter.Config;
import org.apache.calcite.tools.Frameworks.ConfigBuilder;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

public class SchemaWithMetadataExample {

    /** Custom Schema implementation */
    public static class MySchema implements Schema {
        private final Map<String, org.apache.calcite.schema.Table> tables = new HashMap<>();

        public MySchema() {
            tables.put("MY_TABLE", new SimpleScannableTable());
        }

        public Map<String, org.apache.calcite.schema.Table> getTableMap() {
            return tables;
        }

        @Override
        public org.apache.calcite.schema.Table getTable(String name) {
            return tables.get(name);
        }

        @Override
        public Set<String> getTableNames() {
            return tables.keySet();
        }

        public Map<String, Schema> getSubSchemaMap() {
            return Collections.emptyMap();
        }

        @Override
        public Set<String> getSubSchemaNames() {
            return Collections.emptySet();
        }

        @Override
        public Schema getSubSchema(String name) {
            return null;
        }

        @Override
        public Expression getExpression(SchemaPlus parentSchema, String name) {
            return null;
        }

        @Override
        public boolean isMutable() {
            return false;
        }

        @Override
        public Schema snapshot(SchemaVersion version) {
            return this;
        }

        @Override
        public @Nullable RelProtoDataType getType(String name) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'getType'");
        }

        @Override
        public Set<String> getTypeNames() {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'getTypeNames'");
        }

        @Override
        public Collection<Function> getFunctions(String name) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'getFunctions'");
        }

        @Override
        public Set<String> getFunctionNames() {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'getFunctionNames'");
        }
    }

    /** Simple table with metadata (row count, schema) */
    public static class SimpleScannableTable extends AbstractTable implements ScannableTable {
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder()
                    .add("ID", typeFactory.createJavaType(Integer.class))
                    .add("NAME", typeFactory.createJavaType(String.class))
                    .build();
        }

        @Override
        public Enumerable<Object[]> scan(org.apache.calcite.DataContext root) {
            return Linq4j.asEnumerable(Arrays.asList(
                    new Object[]{1, "Alice"},
                    new Object[]{2, "Bob"},
                    new Object[]{3, "Carol"}
            ));
        }
    }

    public static void main(String[] args) throws Exception {
        // Root schema
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("MY_SCHEMA", new MySchema());

        // Config with proper trait defs
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("MY_SCHEMA"))
                .parserConfig(SqlParser.config().withLex(Lex.MYSQL))
                .traitDefs(
                        ConventionTraitDef.INSTANCE,
                        RelCollationTraitDef.INSTANCE,
                        RelDistributionTraitDef.INSTANCE
                )
                .build();

        Planner planner = Frameworks.getPlanner(config);

        // SQL
        String sql = "SELECT * FROM MY_TABLE";

        // Parse
        SqlNode parsed = planner.parse(sql);

        // Validate
        SqlNode validated = planner.validate(parsed);

        // Convert to RelNode
        RelRoot relRoot = planner.rel(validated);
        RelNode relNode = relRoot.rel;

        // Print plan
        System.out.println("Logical Plan:");
        System.out.println(RelOptUtil.toString(relNode));

        // Metadata: row count
        RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();
        Double rowCount = mq.getRowCount(relNode);
        System.out.println("Estimated row count: " + rowCount);
    }
}
