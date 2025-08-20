package org.example;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.*;

public class HepExample {

    private static RelNode optimizeWithHep(RelNode rel) {
        HepProgram program = new HepProgramBuilder()
                .addRuleInstance(CoreRules.PROJECT_MERGE)
                .addRuleInstance(CoreRules.FILTER_MERGE)
                .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
                .build();
        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(rel);
        return planner.findBestExp();
    }

    // --- Fake row count ---
    private static double fakeRowCount(RelNode rel) {
        String type = rel.getRelTypeName().toLowerCase();
        if (type.contains("tablescan") && rel.getTable() != null) {
            String name = rel.getTable().getQualifiedName().get(0).toLowerCase();
            if (name.equals("emps")) return 1000.0;
            if (name.equals("depts")) return 100.0;
        }
        if (type.contains("join")) return 50.0;
        return 1.0;
    }

    public static void main(String[] args) throws Exception {
        SchemaPlus root = Frameworks.createRootSchema(true);
        root.add("hr", new ReflectiveSchema(new HrSchema()));

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(root.getSubSchema("hr"))
                .parserConfig(SqlParser.config().withCaseSensitive(false))
                .build();

        Planner planner = Frameworks.getPlanner(config);

        String sql = "SELECT e.name, d.name FROM EMPS e JOIN DEPTS d ON e.deptno = d.deptno";

        SqlNode parsed = planner.parse(sql);
        SqlNode validated = planner.validate(parsed);
        RelNode logical = planner.rel(validated).rel;

        // --- Print logical plan ---
        System.out.println("Logical Plan:\n" + RelOptUtil.toString(logical, SqlExplainLevel.ALL_ATTRIBUTES));

        // --- Fake stats: directly call helper ---
        double rowCount = fakeRowCount(logical);
        System.out.println("FAKE RowCount estimate = " + rowCount);

        // --- Optimize ---
        RelNode optimized = optimizeWithHep(logical);
        System.out.println("Optimized Plan:\n" + RelOptUtil.toString(optimized, SqlExplainLevel.ALL_ATTRIBUTES));

        double optRowCount = fakeRowCount(optimized);
        System.out.println("FAKE RowCount after optimization = " + optRowCount);
    }

    // --- Demo HR schema ---
    public static class HrSchema {
        public final Employee[] emps = {
                new Employee(1, "Alice", 10),
                new Employee(2, "Bob", 20),
                new Employee(3, "Charlie", 10)
        };
        public final Department[] depts = {
                new Department(10, "Sales"),
                new Department(20, "Marketing")
        };
    }

    public static class Employee {
        public final int empid;
        public final String name;
        public final int deptno;
        public Employee(int empid, String name, int deptno) {
            this.empid = empid; this.name = name; this.deptno = deptno;
        }
    }

    public static class Department {
        public final int deptno;
        public final String name;
        public Department(int deptno, String name) {
            this.deptno = deptno; this.name = name;
        }
    }
}
