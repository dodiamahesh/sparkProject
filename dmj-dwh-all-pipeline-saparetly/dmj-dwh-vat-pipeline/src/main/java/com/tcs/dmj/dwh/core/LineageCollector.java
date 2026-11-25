package com.tcs.dmj.dwh.core;

import com.tcs.dmj.dwh.config.ApplicationProperties;
import com.tcs.dmj.dwh.domain.MetaAsset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Finds the table lineage of a load automatically: it follows every loaded column back through the Spark plan
 * to its MongoDB collections (option "collection") and JDBC tables (option "dbtable").
 * A table only used to filter rows (e.g. an ORPHAN lookup) is not a source.
 */
@Component
public class LineageCollector {

    private static final Logger log = LoggerFactory.getLogger(LineageCollector.class);

    private final String mongoDatabase;
    private final String postgresDatabase;

    public LineageCollector(ApplicationProperties properties) {
        this.mongoDatabase = properties.mongo().database();
        this.postgresDatabase = databaseOfJdbcUrl(properties.postgres().url());
    }

    /**
     * Records "every source of rows -&gt; targetTable" in the context. Never fails a load.
     *
     * @param extraSourceTables tables not in the Spark plan, e.g. dimensions looked up inside the SQL
     */
    public void record(PipelineContext context, Dataset<?> rows, String targetTable,Collection<String> extraSourceTables) {
        try {
            MetaAsset target = MetaAsset.postgresTable(postgresDatabase, targetTable);
            Map<String, MetaAsset> sources = sourcesOf(rows);
            extraSourceTables.forEach(table -> {
                MetaAsset asset = MetaAsset.postgresTable(postgresDatabase, table);
                sources.putIfAbsent(asset.key(), asset);
            });
            sources.values().stream()
                    .filter(source -> !source.key().equals(target.key()))
                    .forEach(source -> context.addLineage(source, target));
        } catch (RuntimeException e) {
            log.warn("[job {}] could not read the lineage of {}: {}", context.pipelineRunId(), targetTable, e.toString());
        }
    }

    public MetaAsset mongoCollection(String collection) {
        return MetaAsset.mongoCollection(mongoDatabase, collection, null);
    }

    public MetaAsset postgresTable(String table) {
        return MetaAsset.postgresTable(postgresDatabase, table);
    }

    /** asset key -&gt; asset, for every collection / table with at least one column in the output of rows. */
    Map<String, MetaAsset> sourcesOf(Dataset<?> rows) {
        LogicalPlan plan = rows.queryExecution().analyzed();
        Map<String, MetaAsset> assets = new HashMap<>();
        Map<Long, Set<String>> provenance = provenance(plan, assets, new IdentityHashMap<>());
        Map<String, MetaAsset> sources = new LinkedHashMap<>();
        for (Attribute column : list(plan.output())) {
            provenance.getOrDefault(column.exprId().id(), Set.of())
                    .forEach(key -> sources.putIfAbsent(key, assets.get(key)));
        }
        return sources;
    }

    /** For every output column of {@code node}: the asset keys it is computed from (column id -&gt; keys). */
    private Map<Long, Set<String>> provenance(LogicalPlan node, Map<String, MetaAsset> assets,
                                              Map<LogicalPlan, Map<Long, Set<String>>> memo) {
        Map<Long, Set<String>> known = memo.get(node);
        if (known != null) {
            return known;
        }
        List<LogicalPlan> children = list(node.children());
        Map<Long, Set<String>> result = children.isEmpty()
                ? leafProvenance(node, assets)
                : innerProvenance(node, children, assets, memo);
        memo.put(node, result);
        return result;
    }

    /** A leaf: every column comes from its own asset (or from nothing). */
    private Map<Long, Set<String>> leafProvenance(LogicalPlan leaf, Map<String, MetaAsset> assets) {
        MetaAsset asset = assetOf(leaf);
        Set<String> keys = Set.of();
        if (asset != null) {
            assets.putIfAbsent(asset.key(), asset);
            keys = Set.of(asset.key());
        }
        Map<Long, Set<String>> result = new HashMap<>();
        for (Attribute column : list(leaf.output())) {
            result.put(column.exprId().id(), keys);
        }
        return result;
    }

    private Map<Long, Set<String>> innerProvenance(LogicalPlan node, List<LogicalPlan> children,
                                                   Map<String, MetaAsset> assets,
                                                   Map<LogicalPlan, Map<Long, Set<String>>> memo) {
        List<Map<Long, Set<String>>> perChild = new ArrayList<>();
        Map<Long, Set<String>> fromChildren = new HashMap<>();
        for (LogicalPlan child : children) {
            Map<Long, Set<String>> childProvenance = provenance(child, assets, memo);
            perChild.add(childProvenance);
            fromChildren.putAll(childProvenance);
        }
        return node instanceof Union
                ? unionProvenance(node, children, perChild)
                : columnProvenance(node, fromChildren);
    }

    /** Union: output column i = column i of every child. */
    private Map<Long, Set<String>> unionProvenance(LogicalPlan union, List<LogicalPlan> children,
                                                   List<Map<Long, Set<String>>> perChild) {
        Map<Long, Set<String>> result = new HashMap<>();
        List<Attribute> output = list(union.output());
        for (int i = 0; i < output.size(); i++) {
            Set<String> keys = new HashSet<>();
            for (int c = 0; c < children.size(); c++) {
                Attribute childColumn = list(children.get(c).output()).get(i);
                keys.addAll(perChild.get(c).getOrDefault(childColumn.exprId().id(), Set.of()));
            }
            result.put(output.get(i).exprId().id(), keys);
        }
        return result;
    }

    /**
     * Other nodes: a column of a child is passed through; a new column (alias) gets the assets of the columns
     * its expression uses; anything else gets the assets of every column the node uses.
     */
    private Map<Long, Set<String>> columnProvenance(LogicalPlan node, Map<Long, Set<String>> fromChildren) {
        Map<Long, Expression> definitions = new HashMap<>();
        for (Expression expression : list(node.expressions())) {
            if (expression instanceof NamedExpression named && !(expression instanceof Attribute)) {
                definitions.put(named.exprId().id(), expression);
            }
        }
        Map<Long, Set<String>> result = new HashMap<>();
        for (Attribute column : list(node.output())) {
            long id = column.exprId().id();
            Set<String> passed = fromChildren.get(id);
            if (passed != null) {
                result.put(id, passed);
                continue;
            }
            Expression definition = definitions.get(id);
            Set<String> keys = new HashSet<>();
            for (Attribute used : list((definition != null ? definition.references() : node.references()).toSeq())) {
                keys.addAll(fromChildren.getOrDefault(used.exprId().id(), Set.of()));
            }
            result.put(id, keys);
        }
        return result;
    }

    private MetaAsset assetOf(LogicalPlan leaf) {
        if (leaf instanceof DataSourceV2Relation relation) {
            String collection = relation.options().get("collection");
            if (collection != null) {
                String database = relation.options().getOrDefault("database", mongoDatabase);
                return MetaAsset.mongoCollection(database, collection, null);
            }
        }
        if (leaf instanceof LogicalRelation relation && relation.relation() instanceof JDBCRelation jdbc) {
            String table = jdbc.jdbcOptions().tableOrQuery();
            if (!table.contains("(")) {   // a sub-query is not an asset
                return MetaAsset.postgresTable(databaseOfJdbcUrl(jdbc.jdbcOptions().url()), table);
            }
        }
        return null;
    }

    private static <T> List<T> list(scala.collection.Seq<T> seq) {
        return JavaConverters.seqAsJavaList(seq);
    }

    /** "jdbc:postgresql://host:5433/silver?x=y" -&gt; "silver" */
    static String databaseOfJdbcUrl(String url) {
        String path = url.substring(url.indexOf("//") + 2);
        int slash = path.indexOf('/');
        if (slash < 0) {
            return null;
        }
        String database = path.substring(slash + 1);
        int query = database.indexOf('?');
        return query < 0 ? database : database.substring(0, query);
    }

    /** For tests and logs: "source -&gt; target" per edge. */
    static List<String> describe(PipelineContext context) {
        return context.lineage().stream()
                .map(edge -> edge.source().displayName() + " -> " + edge.target().displayName())
                .toList();
    }
}
