# DATAP.AI short business plan (draft for external review, 2026-09-27)

Recovered from the 2026-09-27/28 strategy session transcript. Reviewed externally the same day; see the phase journals for the review reactions.

**DATAP.AI — short business plan (draft for external review, 2026-09-27)**

**1. One sentence.** DATAP.AI gets companies to a modern data stack and working AI in weeks, not years, with fewer people. Open to any query engine and any AI model, easy to add the next one, and the data and its governance stay in the customer's hands.

**2. Problem.** Adopting the modern data stack and AI takes most companies one to three years and a team they cannot hire. Hosted platforms shorten it but take ownership of the data and lock in one engine and one AI vendor. Enterprises add a second engine and lose lineage and control at the boundary. Companies on legacy ETL and scheduling (Informatica, Control-M) pay large licences they cannot cancel because leaving is impractical.

**3. Product.** An open data and AI layer deployed in the customer's own cloud account:
- The stack, ready in a week: ingestion, Airflow, dbt, Apache Iceberg storage, BI, Text-to-SQL, document and pipeline agents, all behind one console.
- Agents do the pipeline work; people approve through a governance gate; every action is logged.
- One open copy of the data, readable by Athena, Snowflake and Databricks today, with column lineage and evidence that hold across engines and repositories. Adding an engine is configuration, not a migration.
- Any AI model: AWS Bedrock, Anthropic, OpenAI or a local model, chosen by policy for data residency.
- Converters for legacy estates: Control-M to Airflow (working, tested on a 499-folder real estate), Informatica to dbt (MVP planned).

**4. Business value, measured per customer by the assessment.** People and pipelines not needed; weeks from data request to a governed AI-ready dataset; licences and duplicate data copies retired.

**5. Customers.**
- Beachhead: mid-sized regulated companies in Australia (fintech, superannuation, insurance, health, mid-tier lenders), data teams under ten, board pressure to show AI. Buyer: head of data or CTO, can sign a five-figure deal without a committee.
- Enterprise: banks, insurers, telcos, government, reached through partners once references exist. Legacy migrations are the entry point there.
- Channel: consultancies and integrators licensing the converters and lineage tooling for their own enterprise migrations.

**6. Why us.** Hosted platforms cannot say "your cloud, your data, any engine, any model". Engine vendors will not build governance that crosses engines. Vendor migration tools convert code to their engine only. The founder has run Control-M and Informatica in production and built the modern stack and the AI layer, so the hard cases are in the tooling rather than in consulting hours.

**7. Go to market.** Neutral to the customer, partner-led as a business: AWS, Snowflake and Databricks fund adoption and migration and want the workload. Entry offers: "point us at ten tables, live in a week" for the mid-market; a fixed-price assessment (export in, three numbers out) for legacy estates. Then a fixed monthly fee for the layer, conversion priced per job with parity as acceptance, and the layer as an annual subscription for enterprises.

**8. Traction.** Two live sites, four public repos, all claims measured on our own estate. Three engines on one Iceberg copy; lineage across three repositories (12 tables, 75 columns, 46 cross-engine edges); Text-to-SQL on Databricks; Control-M converter passing 127 tests. No paying customer yet.

**9. Ninety days.** Three mid-market conversations from the founder's network, one paid pilot producing the three numbers on someone else's estate.

**10. Ask.** Pre-seed, roughly 500 to 800 thousand AUD for twelve months: one senior engineer, founder salary, partner registrations and travel.

**11. Risks.** Small first deals; hosted competitors are cheaper to start; the pull toward consulting; single founder; enterprise cycles are long.

**Questions for the reviewer.** Does the one-liner state a business benefit clearly enough, or is it still architecture? Is the mid-market beachhead credible at the ask size? Is "open to any engine and model" a buying reason or a feature? What would you need to see before a seed round?
