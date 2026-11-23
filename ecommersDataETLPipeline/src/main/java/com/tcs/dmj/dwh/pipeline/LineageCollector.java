package com.tcs.dmj.dwh.pipeline;

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
 * Finds the TABLE-LEVEL lineage of a load automatically: which MongoDB collections and PostgreSQL tables a
 * Dataset was computed from. It follows every loaded column back through the Dataset's logical plan to the
 * leaves (Spark keeps them, also for cached Datasets), so no pipeline has to declare its table lineage by hand:
 *
 *   MongoDB read  (format "mongodb", option "collection") -&gt; MetaAsset COLLECTION, layer RAW
 *   JDBC read     (PostgresReader, option "dbtable")      -&gt; MetaAsset TABLE,      layer CURATED
 *
 * The edges are kept in the PipelineContext and written to meta.meta_asset / meta.meta_lineage only after the
 * run succeeded (LineageService).
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
     * Records "every source of {@code rows} -&gt; {@code targetTable}" in the context.
     *
     * @param extraSourceTables tables used by the load that are not in the Spark plan, e.g. the dimensions of a
     *                          direct load (surrogate keys looked up inside the SQL)
     */
    public void record(PipelineContext context, Dataset<?> rows, String targetTable,
                       Collection<String> extraSourceTables) {
        try {
            MetaAsset target = MetaAsset.postgresTable(postgresDatabase, targetTable);
            Map<String, MetaAsset> sources = sourcesOf(rows);
            extraSourceTables.forEach(table -> {
                MetaAsset asset = MetaAsset.postgresTable(postgresDatabase, table);
                sources.putIfAbsent(asset.key(), asset);
            });
            sources.values().stream()
                    .filter(source -> !source.key().equals(target.key()))   // e.g. SCD2 reads its own table
                    .forEach(source -> context.addLineage(source, target));
        } catch (RuntimeException e) {
            // lineage is metadata: it must never fail a load
            log.warn("[job {}] could not read the lineage of {}: {}", context.pipelineRunId(), targetTable, e.toString());
        }
    }

    public MetaAsset mongoCollection(String collection) {
        return MetaAsset.mongoCollection(mongoDatabase, collection, null);
    }

    public MetaAsset postgresTable(String table) {
        return MetaAsset.postgresTable(postgresDatabase, table);
    }

    /**
     * key -&gt; asset, for every MongoDB collection / JDBC table whose columns REACH the output of {@code rows}.
     * A table that is only used to keep or drop rows (e.g. an ORPHAN check joined against a dimension, a filter)
     * is not a source: only tables with at least one column in the loaded data are.
     */
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

    /**
     * For every output column of {@code node}: the assets it is computed from (column id -&gt; asset keys).
     *   leaf                  -&gt; its own asset (MongoDB collection / JDBC table), or none
     *   column of a child     -&gt; passed through (Filter, Join, Sort, ...: the rows change, not the column)
     *   new column (Alias in Project / Aggregate / Window) -&gt; the assets of the columns its expression uses
     *   Union                 -&gt; column i = column i of every child
     *   other new columns     -&gt; the assets of every column the node uses (e.g. explode in Generate)
     */
    private Map<Long, Set<String>> provenance(LogicalPlan node, Map<String, MetaAsset> assets,
                                              Map<LogicalPlan, Map<Long, Set<String>>> memo) {
        Map<Long, Set<String>> known = memo.get(node);
        if (known != null) {
            return known;
        }
        Map<Long, Set<String>> result = new HashMap<>();
        List<LogicalPlan> children = list(node.children());
        if (children.isEmpty()) {
            MetaAsset asset = assetOf(node);
            Set<String> keys = asset == null ? Set.of() : Set.of(asset.key());
            if (asset != null) {
                assets.putIfAbsent(asset.key(), asset);
            }
            list(node.output()).forEach(column -> result.put(column.exprId().id(), keys));
        } else {
            List<Map<Long, Set<String>>> childProvenance = new ArrayList<>();
            Map<Long, Set<String>> fromChildren = new HashMap<>();
            for (LogicalPlan child : children) {
                Map<Long, Set<String>> map = provenance(child, assets, memo);
                childProvenance.add(map);
                fromChildren.putAll(map);
            }
            List<Attribute> output = list(node.output());
            if (node instanceof Union) {
                for (int i = 0; i < output.size(); i++) {
                    Set<String> keys = new HashSet<>();
                    for (int c = 0; c < children.size(); c++) {
                        Attribute childColumn = list(children.get(c).output()).get(i);
                        keys.addAll(childProvenance.get(c).getOrDefault(childColumn.exprId().id(), Set.of()));
                    }
                    result.put(output.get(i).exprId().id(), keys);
                }
            } else {
                Map<Long, Expression> definitions = new HashMap<>();
                for (Expression expression : list(node.expressions())) {
                    if (expression instanceof NamedExpression named && !(expression instanceof Attribute)) {
                        definitions.put(named.exprId().id(), expression);
                    }
                }
                for (Attribute column : output) {
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
            }
        }
        memo.put(node, result);
        return result;
    }

    private static <T> List<T> list(scala.collection.Seq<T> seq) {
        return JavaConverters.seqAsJavaList(seq);
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
            if (!table.contains("(")) {                         // a sub-query is not an asset
                return MetaAsset.postgresTable(databaseOfJdbcUrl(jdbc.jdbcOptions().url()), table);
            }
        }
        return null;
    }

    /** "jdbc:postgresql://localhost:5433/silver?reWriteBatchedInserts=true" -&gt; "silver". */
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

    /** For tests and logs: the edges of a context as "source -&gt; target". */
    static List<String> describe(PipelineContext context) {
        return context.lineage().stream()
                .map(edge -> edge.source().displayName() + " -> " + edge.target().displayName())
                .toList();
    }
}
