# LinkedIn post draft — "The interoperable stack wins when governance travels with the data"

**Author:** Donny Zhao · **Visual:** `lineage_explainer_short_v3.gif` (or the 12 s HTML if posting a video) · **Length:** ~280 words
**Reference:** Ed Baidoun's post on the data stack being rebuilt around interoperability (link in first comment, not in the body)

---

Ed Baidoun made a point last week that most of the industry missed: the biggest story in data engineering right now has little to do with AI. The stack is being rebuilt around interoperability.

Source → CDC → open table format → many compute engines. Iceberg becoming infrastructure. Write once, query anywhere. Change engines without moving a terabyte.

He is right. We run exactly that architecture in production: streaming ticks land as Iceberg on S3, Snowflake serves finance, ClickHouse serves the real-time views, same files.

But he also asked the question nobody in the thread answered:

**Can governance and lineage survive across platforms?**

Today the honest answer is no. Every vendor governs its own garden beautifully. Column lineage, classification, masking — all inside the walls. The moment you move a table to open storage to escape lock-in, you lose them. That is the tax on going open, and it is why so many teams hesitate even when the economics are obvious.

So we built the missing piece. Column-level lineage computed from the pipelines themselves, across every engine. Personal data classified once and followed through every rename, join and hash. Quality tests generated with the pipeline. All of it stored in the customer's own tables, inside their VPC, in their region — and the pipelines themselves written and repaired by AI, so you do not need a platform team to run the interoperable stack.

Below: one click on a number, and its provenance across four raw columns, three tables, two layers. Generated, not drawn.

The interoperable stack wins. It wins the day governance travels with the data.

#dataengineering #apacheiceberg #datagovernance #lineage #sovereignty #APAC

---

**First comment (post immediately after):** Ed's original post: <link>. Live view of the lineage: https://dbt.datap.ai/assets/lineage.html

**Notes**
- No vendor comparison, no product-update name-dropping, no time or price claims.
- "platform" is used only for the customer's platform; we are the missing piece / layer.
- If the GIF is too heavy for LinkedIn, post the three stills (scene 1, 2, 3) as a carousel.
