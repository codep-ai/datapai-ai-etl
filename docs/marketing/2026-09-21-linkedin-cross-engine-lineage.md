# LinkedIn post draft — decouple data from the engine (data sovereignty), claim-safe

**Author:** Donny Zhao · **Visual:** `datapai-platform-be/docs/demo/2026-09-21-cross-engine-lineage-3-engines.png` · **Length:** ~210 words
**Link (first comment, once published):** the 3-minute note, which links to the full engineering note and the live lineage.

---

A business wants to own its data and treat engines as replaceable. That is what data sovereignty means in practice: decouple the data from the engine.

Open table formats got us halfway. The files are now free of the engine. The governance is not. Lineage, quality evidence and ownership still live inside each engine, so the audit trail stops at every engine boundary, exactly where an auditor looks hardest.

That second half is what we build at DATAP.AI: a layer that carries governance across engines.

We proved it on our own demo environment. The same dbt-core models on every engine. Shared tables stored once on Apache Iceberg. Produced on Athena, consumed on Snowflake and on Databricks in another cloud. No table data copied between engines. And one column-level lineage across all three: in this project, 46 column edges cross an engine boundary, none unresolved, held in our own tables and queryable in SQL.

Next, the same idea for AI: models you choose, in your own cloud, answering only from data whose lineage you hold.

It is a demonstration on sample data, not a benchmark and not a production reference. The note includes what broke.

#dataengineering #apacheiceberg #dbt #datalineage #datagovernance

---

## Claims checklist applied (keep for any edit)
- Value is stated as: the pain, where standard tooling stopped IN OUR BUILD, and what we added. Never as what another vendor "cannot" do.
- AI sovereignty is described as what comes next, not as part of this demonstration.
- Not claimed, because not built across engines: carrying personal-data flags across the boundary; moving a live model between engines with a parity proof.
- Scoped to "our own demo environment" / "in this project"; no universal statements about how products behave.
- No timings, no cost figures, no comparison of vendors, nothing that reads as a benchmark.
- "One dbt project" is a demo detail, not the design: do not lead with it. The multi-team shape (a project per producing team) is not built yet; do not claim it until it has run.
- No statement that a vendor's product "cannot" do something; where a route was unavailable we say what the documentation listed when we built it.
- No promise of results for a reader's environment.
- Not every engine read every file directly (Databricks read Athena's tables through Snowflake) — stated in the note, not hidden.
- Not every table is Iceberg (Databricks' own feature tables are its default format in Azure) — stated in the full note.
- Sample market data, described as sample data; no ticker or price quoted.
- Trademark and non-affiliation note on both web versions.
