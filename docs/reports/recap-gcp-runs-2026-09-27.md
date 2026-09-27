# Récap des ajustements depuis le premier run GCP (23/09 → 27/09/2026)

Rédigé à la demande du propriétaire. Le récap couvre 61 commits, de `ae2a735` à `e7be8da`. Les résultats
détaillés sont dans `bench-results/`, et le journal dans `docs/progress.md`.

## Évolution mesurée sur GCP (3 × n2-highmem-8, 50M lignes BigANN)

| | Run 1 (24/09) | Run 5 (25/09) | Run 6 (26/09) | Run 7 (26/09) | Objectif |
|---|---|---|---|---|---|
| Segments par shard | 11–19 | ~9 | ~12 | — | |
| Non filtré p99, stale / linéarisable | 118 / 154 ms | 65 / 68 ms | **34 / 33 ms** | non mesuré | < 100 ms |
| Filtre 1 % p99, stale / linéarisable | 163 / 157 ms | 93 / **118 ms** | **40 / 40 ms** | non mesuré | < 100 ms |
| Débit non filtré, stale / linéarisable | 80 / 67 QPS | 146 / 154 QPS | **301 / 311 QPS** | — | |
| Recall@10, non filtré / filtré | 0,985 / 0,990 | 0,985 / 0,991 | 0,986 / 0,989 | — | |
| Suppression visible partout, p99 | 107 ms | 107 ms | 108 ms | — | < 1 s |
| Ingestion | 8 738 docs/s* | 6 839 docs/s | 6 493 docs/s | 5 542 docs/s | |

\* Le chiffre du run 1 ne se compare pas aux autres. Il couvre seulement les lignes 15M à 50M, sans
fusion, après un redémarrage. Au run 1, un compactage fait après coup (3 segments par shard) donnait
36 / 48 ms en non filtré et 82 / 108 ms en filtré.

Sources : `phase4-gcp-bigann50m.md`, `phase4-gcp-bigann50m-compacted.md`,
`phase4-gcp-bigann50m-2026-09-25.md`, `phase4-gcp-bigann50m-2026-09-26.md`,
`phase4-gcp-bigann50m-run7-ingest.md`.

**Constat.**
- **Latence** : à nombre de segments comparable (runs 1 et 6), le p99 non filtré est divisé par 3,5
  à 4,5, le p99 filtré par 4, et le débit multiplié par 4. Tous les objectifs de latence à 50M sont
  atteints depuis le run 6.
- **Qualité et suppression** : le recall et le délai de suppression sont restés stables.
- **Ingestion** : elle n'a pas progressé sur GCP.

## Les ajustements, par thème

### 1. Solidité du consensus (bugs révélés par le run 1 et la simulation)

- **Contrôle de flux Raft** (`962175e`, `b3f28d9`) : au run 1, 42 Go de messages se sont accumulés
  et le processus a été tué par manque de mémoire. Le leader limite maintenant ce qu'il envoie à
  chaque follower.
- **Ordre d'écriture sur disque** (`b3f28d9`) : les entrées Raft sont désormais écrites avant l'état
  du nœud. L'ordre inverse était un bug de durabilité.
- **Perte de leadership** (`25684f1`) : les écritures mises en attente par un leader qui perd son
  rôle sont redirigées. Avant, les clients restaient bloqués.
- **Snapshots** (`cd90b3f`, `5a3a787`) : une réponse de snapshot ne signale plus que la partie
  validée du journal (correctif de sûreté), et l'installation d'un snapshot résiste à un crash.
- **Redémarrage** : l'index de validation Raft ne peut plus revenir en arrière (ADR 0027).
- **Preuve** : la campagne de simulation est passée à 60 000 seeds sans aucune violation, dont les
  deux tiers avec des disques lents.

### 2. Construction des segments (ADR 0016 à 0021)

- **Le leader construit, les followers récupèrent** (`a11c17b`) : chaque segment est construit une
  seule fois au lieu de trois.
- **Rattrapage d'un nœud en retard** (`6847f80`) : il ne reçoit que les segments qui lui manquent.
- **Constructions parallèles et déterministes** (`9db8fbb`) : une construction utilise plusieurs
  threads, avec un résultat identique quel que soit leur nombre.
- **Équilibrage des leaders et fusions** (`0cf0293`, `61c9b25`) : les leaders sont répartis entre
  les nœuds, et les fusions passent par le journal Raft.
- **Récupérations en pipeline** (`94d9bba`) : 4 fichiers à la fois, avec 8 morceaux en vol par
  fichier.
- **Correctifs issus du run 5** :
  - un leader lent n'est plus traité comme défaillant (`e97f01e`) ;
  - une fusion ne prend jamais le dernier slot libre, qui reste réservé aux flushs (`e5a466f`) ;
  - les fichiers de fusion survivent à un redémarrage (`9349678`).
- **Derniers correctifs** :
  - les threads de construction utilisent toute la machine en priorité basse (`662a8eb`) ;
  - un flush n'attend plus la fusion de son propre shard (`7b781a0`).

### 3. Latence des requêtes (ADR 0025 et 0026) : ce qui a permis d'atteindre l'objectif

- **Recherches hors de l'acteur** (`af95ec9`, `67cc4ff`) : elles tournent sur un pool de threads
  borné. Avant, chaque shard traitait ses recherches une par une dans son acteur de réplica.
- **Chargement des segments allégé** (`27d5ffc`, `ff173ff`) : les segments SQ8 ne lisent plus leur
  colonne f32, et les index sont préparés en dehors de l'acteur.

### 4. Blocages de l'acteur pendant l'ingestion (ADR 0027)

- **Persistance Raft asynchrone** (`d215697`) : les syncs du journal ne bloquent plus l'acteur.
- **Publication et récupération** (`9164580`, `3f4e21d`) : leurs syncs et leurs I/O disque ne se
  font plus sur l'acteur.
- **Correctif de fichiers ouverts** (`a7520b9`) : ce bug venait de l'étape précédente et a été
  révélé par le run 7.
- **Résultat** : l'acteur n'est plus bloqué qu'environ 0,4 % du temps, contre des blocages de 0,3
  à 1,7 s par sync avant.

### 5. Produit et sécurité

- mTLS entre nœuds et protocole versionné (ADR 0018).
- Image Docker et fichiers compose (ADR 0022).
- API HTTP/JSON avec son OpenAPI (ADR 0023). La recherche par filtre seul, qui ne renvoyait rien à
  travers le cluster, a été corrigée au passage.
- Fichiers open source : licence, CONTRIBUTING, SECURITY, code de conduite, CI.
- ADR 0024 (ingestion Kafka) proposé pour après la publication.

### 6. Opérations cloud

- **Scripts GCP** : les clés SSH des IP réutilisées sont oubliées, et le script échoue clairement au
  bout de 10 min si SSH ne répond pas. Ça corrige l'incident des 10,7 h.
- **Garde-fou** : une suppression automatique de la flotte à heure fixe, lancée dès le début de
  chaque run.
- **Coût cumulé connu** : environ 47 USD pour les runs 5, 6 et 7, dont 21 USD perdus sur l'incident
  SSH. Le coût du run 1 n'a pas été chiffré.

## Ingestion : pourquoi elle n'a pas bougé sur GCP

Chaque correction a déplacé le goulot à l'étape suivante :

1. **Syncs sur l'acteur** : corrigé par l'ADR 0027. L'acteur ne bloque plus, mais le run 7 n'en a
   pas profité.
2. **Constructions limitées à la moitié du CPU** : pendant les pauses, les nœuds n'utilisaient que
   1,5 à 3,8 cœurs sur 8.
3. **Flush bloqué par la fusion de son propre shard** : c'est ce qui arrêtait toute l'ingestion.

Les points 2 et 3 sont corrigés depuis le 27/09. En local, sur 6M lignes, la médiane passe de 386 s
à 289 s, soit +33 % (20,8k docs/s), sans blocage de plus de 110 s
(`bench-results/phase4-build-priority-and-slots.md`). Ça ne repose que sur 3 runs, sur une seule
machine, et reste à confirmer sur GCP à 50M.

## Ce qui reste ouvert

- **Le prochain run GCP** confirmera ou non le gain d'ingestion à 50M. Avant, il faut une vraie pause
  des fusions pour pouvoir mesurer les requêtes sur un cluster au repos.
- **Latence des requêtes pendant les constructions** : non mesurée proprement. Elle va de 20 ms à
  6 s dans toutes les variantes.
- **Nombre de documents** : deux runs locaux comptent un peu plus de documents vivants que de lignes.
  Pas encore examiné.
- **Seeds sans progression** : environ 0,1 % des seeds simulés avec des syncs de 150 ms n'acquittent
  aucune écriture. C'est un problème de disponibilité, pas de sûreté.
- **Bonus hors objectif** : les fichiers construits mais pas encore publiés sont perdus au
  redémarrage.
