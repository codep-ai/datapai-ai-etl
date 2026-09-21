# LinkedIn post draft — cross-engine column lineage (short, claim-safe)

**Author:** Donny Zhao · **Visual:** `datapai-platform-be/docs/demo/2026-09-21-cross-engine-lineage-3-engines.png` · **Length:** ~170 words
**Link (first comment, once published):** the 3-minute note, which links to the full engineering note and the live lineage.

---

Data teams are often advised to standardise on one engine. We wanted to see how far the alternative goes, so we built it on our own demo environment.

One dbt-core project. Shared tables stored once on Apache Iceberg. Produced on Athena, consumed on Snowflake and on Databricks in another cloud. No table data copied between engines.

The part we cared about most: column-level lineage that continues across the engine boundary. In this project, 75 column edges, 46 of them crossing from one engine to another, none unresolved. Click a column on Databricks and follow it back through a Snowflake mart and the Athena producer to the raw source.

Three things we learned the hard way are in the note — including a table that built successfully, passed its tests, and held zero rows.

It is a demonstration on sample data, not a benchmark and not a production reference. What it shows is that the approach works end to end.

#dataengineering #apacheiceberg #dbt #datalineage #datagovernance

---

## Claims checklist applied (keep for any edit)
- Scoped to "our own demo environment" / "in this project"; no universal statements about how products behave.
- No timings, no cost figures, no comparison of vendors, nothing that reads as a benchmark.
- No statement that a vendor's product "cannot" do something; where a route was unavailable we say what the documentation listed when we built it.
- No promise of results for a reader's environment.
- Not every engine read every file directly (Databricks read Athena's tables through Snowflake) — stated in the note, not hidden.
- Not every table is Iceberg (Databricks' own feature tables are its default format in Azure) — stated in the full note.
- Sample market data, described as sample data; no ticker or price quoted.
- Trademark and non-affiliation note on both web versions.
