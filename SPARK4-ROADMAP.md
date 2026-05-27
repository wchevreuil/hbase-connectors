# Roadmap: Spark 4 support for the HBase Spark connector

This document describes **which capabilities of `hbase-spark` to preserve**, how to **introduce Spark 4 without retiring Spark 3**, and a **phased implementation plan**. It is meant to guide development in Apache HBase connectors (JIRA / discuss@hbase as needed).

## Goals

1. **Feature parity** for the behaviors users rely on today (catalog, read/write DataFrame API, pushdown, `HBaseContext`, bulk load, Java API).
2. **Isolation**: Spark 3 stays on the existing **`spark/`** reactor (`hbase-spark`, `hbase-spark-it`). Spark 4 lives in a **separate Maven reactor** under the repo root so `spark.version`, `scala.version`, compiler flags, and optional Spark-4-only APIs never collide with Spark 3 in a single parent POM.
3. **Reuse** shared, non–Spark-version-specific artifacts where safe (typically **protocol** / protobuf modules).

## Recommended repository layout

### Root `pom.xml`

- Add an optional aggregator module, e.g. **`<module>spark4</module>`**, either unconditional or behind a Maven profile (e.g. `-Pspark4`) if CI initially builds Spark 3 only.

### New directory: `spark4/` (sibling to `spark/`)

| Module | Purpose |
|--------|--------|
| **`spark4/pom.xml`** | Parent POM: `spark4.version`, **Scala 2.13.x** (per Spark 4 requirement), Spark 4 deps, shared plugin config for this line only. |
| **`spark4/hbase-spark4`** | Main connector (port of `spark/hbase-spark`). Artifact id example: **`hbase-spark4`**, `groupId`: `org.apache.hbase.connectors.spark` (or `spark4` subgroup—decide for release clarity). |
| **`spark4/hbase-spark4-it`** | Integration tests mirroring `spark/hbase-spark-it` against Spark 4. |

### Shared modules (reuse vs fork)

| Existing module | Recommendation |
|-----------------|----------------|
| **`spark/hbase-spark-protocol`** | **Reuse** as a normal dependency if generated Java and public API remain compatible with Spark 4 classpath; no Scala version in that jar. |
| **`spark/hbase-spark-protocol-shaded`** | **Reuse** for **`SparkFilterProtos`** (Protobuf relocated to **`org.apache.hbase.thirdparty`**). Shade runs only at **`package`**, so a clean reactor build of **`spark4`** should run at least **`mvn … package`** (or **`install`**) before **`SparkSQLPushDownFilter`** compiles, or reuse a previously packaged jar from **`~/.m2`**. **`dev-support/build-spark4.sh`** uses **`clean install`**.
| **`spark/hbase-spark`** | **Do not branch in-place** for Spark 4; keep as the Spark 3 connector. **`hbase-spark4`** does **not** depend on **`hbase-spark`** (Scala **2.12 / 3** vs **2.13 / 4**). |
| **`spark/hbase-spark-pushdown_${scala.binary.version}`** | **Reuse** (“shared pushdown”) — sources under **`spark/hbase-spark-pushdown`**; **`spark4/hbase-spark-pushdown`** rebuilds them for Scala **2.13** / Spark **4**. Exposes **`SparkSQLPushDownFilter`**, **`PushdownMappedField`**, **`DynamicLogicExpression`**, **`ByteArrayComparable`**, **`JavaBytesEncoder`**, **`BytesEncoder`** in **`org.apache.hadoop.hbase.spark`**. **`hbase-spark`** and **`hbase-spark4`** depend on **`hbase-spark-pushdown_${scala.binary.version}`**. |

If a future change breaks protocol compatibility, introduce **`hbase-spark4-protocol`** only then; default is one protocol for both lines.

### `SparkSQLPushDownFilter`, `SparkFilterProtos`, and avoiding a duplicate implementation

- **Protobuf (`SparkFilterProtos`)** lives in **`spark/hbase-spark-protocol`** (`groupId`: **`org.apache.hbase.connectors.spark`**, **`hbase-spark-protocol`**). Consume the wire format through **`spark/hbase-spark-protocol-shaded`** (`hbase-spark-protocol-shaded`) so Spark 3 and Spark 4 share **one** definition on the RegionServer—no duplicated `.proto` or parallel serializer code.
- **Server-side filter logic** **`SparkSQLPushDownFilter`** is **`spark/hbase-spark-pushdown`** (Java): **`spark/hbase-spark-pushdown/src/main/java/org/apache/hadoop/hbase/spark/SparkSQLPushDownFilter.java`**. **`hbase-spark`** and **`hbase-spark4`** both consume it via **`hbase-spark-pushdown_${scala.binary.version}`**. **`SerializedFilter`** in **`HBaseTableScanRDD`** protobuf-serializes the filter (`toByteArray` / **`parseFrom`**) onto task partitions exactly like **`spark/hbase-spark`**.
- **`hbase-spark`** vs **`hbase-spark4`**: connectors stay separate artifacts (Scala **2.12** vs **2.13**); **share no duplicate Java** for **`SparkSQLPushDownFilter`**—only **`hbase-spark-pushdown`** carries that bytecode per Scala line.

### Assembly / packaging

- Extend **`hbase-connectors-assembly`** (or add `hbase-connectors-assembly-spark4`) when you are ready to publish Spark-4-specific convenience bundles; keep Spark 3 assembly unchanged until then.

---

## Features to keep (parity checklist)

Treat each row as a **migration work item** in `hbase-spark4`.

### A. Data Source / DataFrame / SQL

| Feature | Notes for Spark 4 |
|--------|---------------------|
| Short name / FQCN registration | Keep **`org.apache.hadoop.hbase.spark`** only if Spark 4 service loader rules still allow it; otherwise document FQCN change and migration note. |
| JSON **table catalog** (`catalog` option) | Preserve schema: table namespace/name, composite **rowkey**, columns (`cf`, `col`, `type`, Avro, serdes, length). |
| Legacy **`hbase.table` / `hbase.columns.mapping`** | Keep conversion path or explicit deprecation with one release overlap. |
| **Read**: column pruning, scan construction | Port `PrunedFilteredScan` / relation logic to Spark 4 Catalyst types. |
| **Filter / row-key pushdown** | Preserve semantics; update expression → `SparkSQLPushDownFilter` translation for any Catalyst changes. |
| **Server-side filter** (`SparkSQLPushDownFilter`) | **`hbase-spark-pushdown_${scala.binary.version}`** + **`hbase-spark-protocol-shaded`** (`SparkFilterProtos`); see **`SparkSQLPushDownFilter`, `SparkFilterProtos`, and avoiding a duplicate implementation**. |
| **Time / versions** (`timestamp`, timerange, `maxVersions`) | Same option keys as `HBaseSparkConf` where practical for user migration. |
| **Scan tuning** (cache blocks, cached rows, batch size, bulk get size) | Same `SparkConf` / option keys. |
| **Encoder plug-in** (`hbase.spark.query.encoder`) | Keep `JavaBytesEncoder` / default encoder behavior. |
| **Write** via `Put` + MR output | Today: `saveAsHadoopDataset` + `TableOutputFormat`; verify Spark 4 still supports this path or provide equivalent (`RDD`/`Dataset` write path). |
| **InsertableRelation** / save modes | Map `SaveMode` behavior explicitly (current code paths may need `SparkSession`-aware write builder). |
| **Create table on write** (`newtable`, region splits, start/end key options) | Preserve admin DDL behavior. |
| **`HBaseContext` integration** (`hbase.spark.use.hbasecontext`, config location) | Keep behavior for sharing connections with imperative API. |

### B. `HBaseContext` (imperative API)

| Feature | Notes |
|--------|--------|
| Broadcast `Configuration`, credentials for MR | Align with Spark 4 security / Hadoop config APIs. |
| `foreachPartition` / `mapPartitions` (RDD) | Core API port (Scala 2.13, `SparkContext`). |
| `bulkPut`, `bulkDelete`, `bulkGet` | Buffered mutator / batch patterns unchanged semantically. |
| `hbaseRDD` + `TableInputFormat` / `NewHBaseRDD` | Confirm `NewHadoopRDD` / input format wiring on Spark 4. |
| `bulkLoad` / `bulkLoadThinRows` | HFile generation + partitioner logic; Hadoop FS/HBase APIs. |
| **Java** `JavaHBaseContext` | Port to Spark 4 `JavaSparkContext` / entry points. |

### C. Implicit extensions

| Feature | Notes |
|--------|--------|
| `HBaseRDDFunctions` | Same method names and signatures where binary compatibility matters for Scala users. |
| `HBaseDStreamFunctions` | **Re-evaluate**: Spark **Streaming (DStream)** may be legacy; either port as-is for parity or document “Structured Streaming only” for new code. |

### D. Supporting infrastructure

| Feature | Notes |
|--------|--------|
| `HBaseConnectionCache` / connection TTL | Preserve semantics. |
| `LatestHBaseContextCache` | Used by datasource + context; keep consistent. |
| Avro / complex types in catalog | `SchemaConverters`, `Field`, examples. |
| Examples / tests (Scala + Java) | Duplicate under `spark4` or symlink—prefer copies under `hbase-spark4` to avoid wrong dependency. |

---

## Phased implementation plan

### Phase 0 — Scaffolding (no feature parity yet)

- Add **`spark4/`** parent and **`hbase-spark4`** empty module with correct **`provided`** Spark 4 + Scala 2.13 dependencies.
- Depend on **`hbase-spark-protocol-shaded`** and **`spark4/hbase-spark-pushdown`** (recompile of **`spark/hbase-spark-pushdown`** for Scala **2.13**) from the **`spark/`** protobuf + shared sources.
- Wire root and CI (profile or matrix job) so **Spark 3 and Spark 4 builds both succeed independently**.

**Build tooling:** Use **Maven 3.9+** (Surefire 3.5.x in this repo requires a recent Maven; **3.9.9** matches Spark 4.0 build guidance). On macOS with Homebrew, prefer `/opt/homebrew/opt/maven/bin/mvn`, or run `dev-support/build-spark4.sh` (it defaults to that path when executable). Override with `MVN=/path/to/mvn`.

**Exit criteria:** `mvn -Pspark4 -DskipTests clean install` (from the repo root, with JDK 17+) builds **`spark4`** (including **`hbase-spark-pushdown_2.13`** and **`hbase-spark4`**). The `spark4` reactor is only on the build graph when profile **`spark4`** is active.

**Catalog schema stack (started):** JSON table catalog and related parsers/converters mirror Spark 3 layout: **`Logging`** and **`SchemaConverters`** in **`org.apache.hadoop.hbase.spark4`**; **`HBaseTableCatalog`**, **`DataTypeParserWrapper`** (Spark 4 **`DataTypeParser`** vs Spark 3 **`CatalystSqlParser`**), and **`SerDes`** in **`org.apache.hadoop.hbase.spark4.datasources`**.

### Phase 1 — Core runtime (minimal slice delivered)

**Delivered today (narrow “Phase 1 + catalog path” slice):**

- **`HBaseContext`** (minimal): broadcast config, **`hbaseRDD(TableName, Scan)`** via **`NewHBaseRDD`**; **`LatestHBaseContextCache`** for datasource reuse.
- **`HBaseConnectionCache`**, **`NewHBaseRDD`**, **`ByteArrayComparable`**, datasource helpers (**`SerializableConfiguration`**, **`HBaseTableScanRDD`**, encoders/utilities).
- **`DefaultSource`** / **`HBaseRelation`**: **`SparkSQLPushDownFilter`** wired when **`hbase.spark.pushdown.columnfilter`** is true (`buildScan` mirrors Spark **`hbase-spark`**); **`HBaseTableScanRDD.SerializedFilter`** protobuf round-trip with **`SparkFilterProtos`** via **`spark/hbase-spark-protocol-shaded`**.
- **Unit tests:** **`HBaseCatalogSuite`** plus **`HBaseSpark4DataSourceCatalogSuite`** (Spark **`SparkSession`** + **`DefaultSource`**, asserts **`.schema`** only—no **`count`** / no MiniCluster).

**Still open for Phase 1 parity:**

- **`HBaseRDDFunctions`**, **Java `JavaHBaseContext`**, and integration tests (**bulkPut** / **bulkGet** / **bulkDelete**, **`hbaseRDD`** against HBase MiniCluster).

**Exit criteria (full Phase 1):** Bulk put/get/delete + `hbaseRDD` integration tests pass on Spark 4.

### Phase 2 — Bulk load

- Port **`bulkLoad`**, **`bulkLoadThinRows`**, **`BulkLoadPartitioner`**, related types (`KeyFamilyQualifier`, `FamilyHFileWriteOptions`, etc.).

**Exit criteria:** `BulkLoadSuite`-equivalent tests pass.

### Phase 3 — DataSource / `HBaseRelation`

- **Column predicate pushdown to RegionServers** via **`SparkSQLPushDownFilter`** is implemented ( **`hbase-spark-pushdown_2.13`** ); extend with **`DefaultSourceSuite`** / IT coverage and **`SparkSession`** migration where still using **`SQLContext`** internally.

**Exit criteria:** `DefaultSourceSuite`-equivalent and catalog suites pass; filter pushdown + protobuf path verified.

### Phase 4 — Streaming (optional / decision)

- If DStream remains a requirement: port **`HBaseDStreamFunctions`** and streaming paths in **`HBaseContext`**.
- Else: document deprecation and provide a minimal migration note toward Structured Streaming + batch `HBaseContext` (future work).

**Exit criteria:** Explicit decision recorded in README + tests or documented gap.

### Phase 5 — Integration tests, docs, release

- Port **`hbase-spark-it`** to **`hbase-spark4-it`**.
- Update user-facing docs: coordinates **`hbase-spark4`**, Spark / Scala / HBase / Hadoop matrix, migration from `hbase-spark`.
- Assembly / release notes for downstreams.

**Exit criteria:** Release candidate checklist and vote thread material ready.

---

## Risk register (short)

| Risk | Mitigation |
|------|------------|
| **DataSource v1** deprecation | Plan DSv2 (`TableProvider`) as a follow-up if Spark 4 deprecates v1; Phase 3 can still target v1 for faster parity. |
| **Catalyst internal API changes** | Pushdown translation is highest churn; budget extra QA on filter combinations. |
| **Scala 2.13** syntax / collections | Mechanical port + compiler; implicits and numeric widening need review. |
| **DStream dependency** | Confirm `spark-streaming` artifact availability and policy for Spark 4.x. |

---

## Summary

- **Keep:** Catalog, read/write DataFrame behavior, pushdown, encoders, `HBaseContext` (RDD + bulk load), Java API, and tests/examples for those surfaces.
- **Separate module:** Introduce a **`spark4/`** Maven reactor with **`hbase-spark4`** (+ IT), reuse **protocol shaded** jars, leave **`spark/hbase-spark`** as the supported **Spark 3** line.
- **Deliver in phases:** scaffolding → RDD/context → bulk load → datasource → streaming decision → IT + release.

When this roadmap is adopted, open a parent JIRA (e.g. “Add Spark 4 connector module”) and child tickets per phase / parity row above.
