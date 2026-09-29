# Use cases by sector

What developers build with a hybrid search database, sector by sector, and what each use asks
of Cairn. It drives ADR 0031 (natural wiring). Written 2026-09-29, from the owner's request:
"le moteur n'est pas au point, il faut un câblage naturel pour la majorité des usages".

## By sector

| sector | use | data and typical queries | what it asks of Cairn |
|---|---|---|---|
| **AI agents and assistants** | long-term memory of an agent or assistant | short memories per user; "what do I know about this user on this topic" | the agent reads what it just wrote (read-your-writes); **"forget me" erases one user entirely**; strict isolation between users |
| **B2B SaaS, customer support** | RAG over each customer's knowledge base and tickets | documents split into chunks; filters on customer, product, date | **strict multi-tenancy**; deleting a whole customer when the contract ends; frequent article updates |
| **Legal, legaltech** | RAG over contracts, case law, matters | long documents in chunks; filters on matter, client, jurisdiction, date | **access rights per matter**; professional secrecy; provable deletion at the end of a mandate |
| **Health** | similar cases, RAG over protocols, patient records | clinical notes; filters on patient, department, date | GDPR and health-data hosting; **withdrawn consent means verifiable erasure**; sovereign hosting |
| **Banking, insurance** | adviser assistant, similar claims, compliance (KYC) | customer files; filters on product, status, dates | **legal retention, then deletion when it expires**; audit of who deleted what |
| **Media, archives, rights** | text, image and audio search; rights management; takedowns (Cairn's origin) | several vectors per document (image, transcript); filters on territory and rights period | **a takedown is immediate everywhere**; multimodal search |
| **E-commerce, retail** | hybrid product search, recommendations | catalogue; filters on price, stock, category, brand | **frequent partial updates** (price, stock) without rewriting the document; immediate removal of a product |
| **HR, recruiting** | matching CVs and job offers | CVs and offers; filters on location, skills, availability | GDPR: erasing candidates on request or when retention ends |
| **Industry, maintenance** | technical documentation, similar incidents | manuals, incident reports; filters on equipment, site | **on-premises**, off the cloud; long documents |
| **Public sector** | RAG over administrative texts, services to citizens | regulations; filters on administration, date | **open source, sovereignty**; archiving obligations against the right to be forgotten |
| **Cybersecurity** | similar events or indicators | continuous streams (logs, alerts); filters on time and source | **stream ingestion** (Kafka); automatic expiry of old data |
| **Education, research** | RAG over corpora, plagiarism detection | papers, courses; filters on author, year | large read volumes; few deletions |

## Needs across sectors

Ordered by how many sectors need them.

1. **Text identifiers.** Everyone has UUIDs or business ids. Cairn requires 64-bit integers
   today.
2. **Deletion by criterion**, not only by id: everything of this customer, this matter, this
   user, this parent document. This is the core of the deletion promise, and Cairn deletes by
   id only today.
3. **Parent documents and chunks.** In RAG, a document becomes N chunks. Deleting the document
   must delete every chunk, and a search should be able to return the document.
4. **Multi-tenancy.** Isolation per customer or per user, ideally enforced by the server from
   the API key, so an application cannot read a neighbour tenant by mistake.
5. **Collections created through the API**, each with its schema and vector dimension. Cairn
   has one schema per cluster, fixed at startup, today.
6. **Clients and integrations**: Python and TypeScript clients, and above all LangChain and
   LlamaIndex, through which most RAG developers work. With a vector-store integration, Cairn
   plugs in with three lines.
7. **Automatic retention**: expiry after a duration (banking, health, HR, security).
8. **Partial updates**: e-commerce, and metadata in general.
9. **Stream ingestion** from Kafka (ADR 0024, proposed).
10. **Exportable proof of deletion**: an audit log today, a verifiable report later.

**Already covered:**
- hybrid search with filters;
- several vectors per document;
- read-your-writes;
- takedowns that are immediate everywhere and audited;
- self-hosted open source.

## What follows

Needs 1 to 6 together make the wiring natural for most uses. ADR 0031 designs them, with the
TypeScript and Python clients. Needs 7 to 10 come after.
