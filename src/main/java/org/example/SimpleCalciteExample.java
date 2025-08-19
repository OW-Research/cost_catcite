package org.example;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.DataContext;

import java.util.Collections;
import java.util.Properties;

public class SimpleCalciteExample {

    public static void main(String[] args) throws Exception {
        // --- SQL ---
//        final String sql = "SELECT u.id, p.name FROM USERS u,  PRODUCTS p WHERE u.id = p.id AND u.id > 10 "; 
        final String sql = "SELECT u.id, p.name FROM  USERS u ,   PRODUCTS p WHERE u.id = p.id  and u.id>20"; 
        //"ORDER BY u.id" i smissing logicalSORT
        // --- Parse ---
        SqlParser.Config parserConfig = SqlParser.config().withCaseSensitive(false);
        SqlNode parsed = SqlParser.create(sql, parserConfig).parseQuery();

        // --- Schema with two tables ---
        SchemaPlus root = Frameworks.createRootSchema(true);
        root.add("USERS", new CustomTable("Users",200.0)); // 100 rows
        root.add("PRODUCTS", new CustomTable("Product",8_000_000.0)); // 1,000,000 rows

        // --- Catalog + Validator ---
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(root),
                Collections.singletonList(root.getName()),
                typeFactory,
                new CalciteConnectionConfigImpl(new Properties()));

        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(),
                catalogReader,
                typeFactory,
                SqlValidator.Config.DEFAULT);
        SqlNode validated = validator.validate(parsed);

        // --- Planner & Cluster ---
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);

        // Enumerable rules to produce a physical plan
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);

        // Some logical rules that may change join order
        planner.addRule(CoreRules.JOIN_COMMUTE);
        planner.addRule(CoreRules.JOIN_ASSOCIATE);
        planner.addRule(CoreRules.FILTER_INTO_JOIN); //to push filters into joins

        // Cluster
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        // --- SQL to Rel ---
        SqlToRelConverter sqlToRel = new SqlToRelConverter(
                (type, query, schema, path) -> null, // no view expansion
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());

        RelRoot rootRel = sqlToRel.convertQuery(validated, false, true);
        RelNode logical = rootRel.rel;

        System.out.println("---- Initial Logical Plan ----");
        System.out.println(RelOptUtil.dumpPlan(
                "Initial Logical Plan", logical, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));

        // --- Ask for EnumerableConvention and optimize ---
        RelTraitSet desired = cluster.traitSet().replace(EnumerableConvention.INSTANCE);
        RelNode converted = planner.changeTraits(logical, desired);
        planner.setRoot(converted);
        RelNode best = planner.findBestExp();

        System.out.println("\n---- Optimized Plan ----");
        System.out.println(RelOptUtil.dumpPlan(
                "Optimized", best, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    /**
     * Simple in-memory table with a fabricated row count.
     * Implements ScannableTable so Enumerable rules can create a physical scan.
     */
    static class CustomTable extends AbstractTable implements ScannableTable {
        final double rowCount;
        final String name;
        CustomTable(String name, double rowCount) {
            this.name = name;
            this.rowCount = rowCount;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            final RelDataTypeFactory.Builder b = typeFactory.builder();
            b.add("ID", typeFactory.createJavaType(int.class));
            b.add("NAME", typeFactory.createJavaType(String.class));
            return b.build();
        }

        // Let Calcite know this is a plain TABLE (helps some tooling)
        @Override
        public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        // Provide basic stats too (not required for the demo, but nice to have)
        @Override
        public Statistic getStatistic() {
            System.err.println("--> Using custom Statistic for "+name+" with rowCount = " + rowCount);
            return Statistics.of(rowCount, ImmutableList.of());
        }

        // Minimal data so a physical Enumerable plan can exist (we won't execute it)
        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            // List<Object[]> rows = Arrays.asList(
            //         new Object[] { 1, "a" },
            //         new Object[] { 2, "b" },
            //         new Object[] { 3, "c" });
            // return Linq4j.asEnumerable(rows);
            return null;
        }
    }
}
