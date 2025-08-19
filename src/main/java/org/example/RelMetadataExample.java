package org.example;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

public class RelMetadataExample {

    /** Custom table with statistics */
    static class MyTable implements Table {
        private final RelDataType rowType;
        private final long rowCount;

        MyTable(RelDataType rowType, long rowCount) {
            this.rowType = rowType;
            this.rowCount = rowCount;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return rowType;
        }

        @Override
        public Statistic getStatistic() {
            return new Statistic() {
                @Override
                public Double getRowCount() {
                    return (double) rowCount;
                }

                public boolean isKey(List<String> columns) {
                    return false;
                }

                @Override
                public @Nullable RelDistribution getDistribution() {
                    return (@Nullable RelDistribution) Collections.emptyList();
                }

                public double getSelectivity(List<String> columns, List<RexNode> predicates) {
                    // Example: return 0.5 for any predicate
                    return 0.5;
                }
            };
        }

        @Override
        public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        public Collection getCollationList() {
            return Collections.emptyList();
        }

        @Override
        public boolean isRolledUp(String column) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'isRolledUp'");
        }

        @Override
        public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, @Nullable SqlNode parent,
                @Nullable CalciteConnectionConfig config) {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'rolledUpColumnValidInsideAgg'");
        }
    }

    /** Simple schema with one table */
    static class MySchema extends AbstractSchema {
        private final RelDataTypeFactory typeFactory;

        MySchema(RelDataTypeFactory typeFactory) {
            this.typeFactory = typeFactory;
        }

        @Override
        protected Map<String, Table> getTableMap() {
            Map<String, Table> map = new HashMap<>();
            RelDataType rowType = typeFactory.builder()
                    .add("EMPNO", typeFactory.createJavaType(Integer.class))
                    .add("NAME", typeFactory.createJavaType(String.class))
                    .build();
            map.put("EMP", new MyTable(rowType, 100));
            return map;
        }
    }

    public static void main(String[] args) throws Exception {
        // 1. Create Calcite connection
        Properties info = new Properties();
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        // 2. Add schema
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("MYSCHEMA", new MySchema(Frameworks.createRelDataTypeFactory()));

        // 3. Build RelNode using RelBuilder
        RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema.getSubSchema("MYSCHEMA"))
                .build());
        builder.scan("EMP");
        RelNode relNode = builder.build();

        // 4. Query metadata
        RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

        // Row count
        Double rowCount = mq.getRowCount(relNode);
        System.out.println("Estimated row count: " + rowCount);

        // Selectivity example (empty predicates)
        Double selectivity = mq.getSelectivity(relNode, (@Nullable RexNode) Collections.emptyList());
        System.out.println("Estimated selectivity (empty): " + selectivity);
    }
}
