import admin from "firebase-admin";
import { BigQuery } from "@google-cloud/bigquery";

/** ========= CONFIG ========= **/
const PROJECT_ID       = process.env.PROJECT_ID;
const BQ_DATASET       = process.env.BQ_DATASET;       // p.ej. analytics_123456789
const BQ_LOCATION      = process.env.BQ_LOCATION || "US";
const DAYS_WINDOW      = parseInt(process.env.DAYS_WINDOW || "30", 10);
const TOP_TAGS_PER_NAT = parseInt(process.env.TOP_TAGS_PER_NAT || "5", 10);
const POSTS_PER_NAT    = parseInt(process.env.POSTS_PER_NAT || "3", 10);

// Colecciones según tu BD
const STUDENT_USER_COLL     = "StudentUser";          // aquí están "id" y "nationality"
const STUDENT_PROFILE_COLL  = "StudentUserProfile";   
const HOUSING_POST_COLL     = "HousingPost";
const HOUSING_TAG_COLL      = "HousingTag";           // catálogo de tags (id, name)
const TAG_SUBCOLL           = "Tag";
const REC_SUBCOLL           = "RecommendedHousingPosts";

/** ========= INIT ========= **/
if (!admin.apps.length) {
  admin.initializeApp({ projectId: PROJECT_ID });
}
const firestore = admin.firestore();
const bigquery  = new BigQuery({ projectId: PROJECT_ID, location: BQ_LOCATION });

/** ========= BIGQUERY: Top tags por nacionalidad ========= **/
async function getTopTagsByNationality() {
  const query = `
  DECLARE date_from DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY);

  WITH events AS (
    SELECT event_date, event_name, event_params, user_properties
    FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.events_*\`
    WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', date_from) AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
    UNION ALL
    SELECT event_date, event_name, event_params, user_properties
    FROM \`${BQ_PROJECT_ID}.${BQ_DATASET}.events_intraday_*\`
    WHERE event_date BETWEEN FORMAT_DATE('%Y%m%d', date_from) AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
  ),
  base AS (
    SELECT
      -- SOLO housing_post_click
      -- user_nationality y housing_category VIENEN EN event_params, según tu AnalyticsHelper
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'user_nationality') AS user_nationality,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'housing_category')  AS housing_category
    FROM events
    WHERE event_name = 'housing_post_click'
  ),
  norm AS (
    SELECT
      LOWER(TRIM(user_nationality)) AS user_nationality,
      LOWER(TRIM(housing_category)) AS housing_category
    FROM base
    WHERE user_nationality IS NOT NULL AND housing_category IS NOT NULL
  ),
  agg AS (
    SELECT user_nationality, housing_category, COUNT(*) AS clicks
    FROM norm
    GROUP BY 1,2
  ),
  ranked AS (
    SELECT
      user_nationality,
      housing_category,
      clicks,
      ROW_NUMBER() OVER (PARTITION BY user_nationality ORDER BY clicks DESC) AS rk
    FROM agg
  )
  SELECT user_nationality, housing_category, clicks
  FROM ranked
  WHERE rk <= @topN
`;

  const [job]   = await bigquery.createQueryJob({ query, params: { days: DAYS_WINDOW, topN: TOP_TAGS_PER_NAT } });
  const [rows]  = await job.getQueryResults();

  // nat -> Set(tagName)
  const map = new Map();
  for (const r of rows) {
    const nat = r.user_nationality;
    const tag = r.housing_category;
    if (!nat || !tag) continue;
    if (!map.has(nat)) map.set(nat, new Set());
    map.get(nat).add(tag);
  }
  return map;
}

/** ========= (Opcional) Cargar catálogo de tags: name <-> id ========= **/
async function loadHousingTagCatalog() {
  const snap = await firestore.collection(HOUSING_TAG_COLL).get();
  const nameToId = new Map();
  const idToName = new Map();
  snap.forEach(doc => {
    const d = doc.data();
    const id = doc.id || d.id;
    const nm = d?.name ? String(d.name).toLowerCase().trim() : null;
    if (id && nm) {
      nameToId.set(String(nm), String(id));
      idToName.set(String(id), String(nm));
    }
  });
  return { nameToId, idToName };
}

/** ========= Posts candidatos por nacionalidad =========
 * Devuelve: nat -> [ { id, title, price, rating, photoPath, reviewsCount } ]
 */
async function getCandidatePostsByNationality(topTagsByNat, tagCatalog) {
  const outMap = new Map();
  for (const nat of topTagsByNat.keys()) outMap.set(nat, []);

  const pageSize = 300;
  let lastDoc = null;

  while (true) {
    let q = firestore.collection(HOUSING_POST_COLL).limit(pageSize);
    if (lastDoc) q = firestore.collection(HOUSING_POST_COLL).startAfter(lastDoc).limit(pageSize);
    const snap = await q.get();
    if (snap.empty) break;

    for (const doc of snap.docs) {
      const post = doc.data();
      const postId = doc.id;

      // Subcolección Tag (id, name)
      const tagSnap = await doc.ref.collection(TAG_SUBCOLL).get();
      if (tagSnap.empty) continue;

      const tagNames = new Set();
      const tagIds   = new Set();

      tagSnap.forEach(t => {
        const td = t.data();
        if (td?.name) tagNames.add(String(td.name).toLowerCase().trim());
        if (td?.id)   tagIds.add(String(td.id));
      });

      // Preview del post con los campos que necesitas en RecommendedHousingPosts
      const preview = {
        id: postId,
        housing: postId,                             // id del post
        title: post?.title ?? "",
        price: post?.price ?? 0,
        rating: post?.rating ?? 0,
        reviewsCount: post?.reviewsCount ?? 0,       // si no existe, 0
        photoPath: post?.thumbnail ?? post?.photoPath ?? "" // ajusta si tu thumbnail se llama distinto
      };

      // Para cada nacionalidad: intersección no vacía (match por nombre y por id)
      for (const [nat, topTagNames] of topTagsByNat.entries()) {
        // name -> id (del catálogo) para reforzar el match por id también
        const topTagIds = new Set();
        for (const nm of topTagNames) {
          const maybeId = tagCatalog.nameToId.get(nm);
          if (maybeId) topTagIds.add(maybeId);
        }
        const matchByName = [...topTagNames].some(nm => tagNames.has(nm));
        const matchById   = [...topTagIds].some(id => tagIds.has(id));
        if (matchByName || matchById) {
          outMap.get(nat).push(preview);
        }
      }
    }

    lastDoc = snap.docs[snap.docs.length - 1];
    if (snap.size < pageSize) break;
  }

  // Ordena por rating desc y corta a 3 por nacionalidad
  for (const [nat, arr] of outMap.entries()) {
    arr.sort((a, b) => (b.rating ?? 0) - (a.rating ?? 0));
    outMap.set(nat, arr.slice(0, POSTS_PER_NAT));
  }
  return outMap;
}

/** ========= Upsert en RecommendedHousingPosts por estudiante =========
 * Recorre StudentUser (id, nationality), resuelve su perfil en StudentUserPorfile por userId,
 * y hace upsert de 3 posts (id = housingId, merge=true).
 */
async function writeRecommendationsForStudents(candidatesByNat) {
  const pageSize = 300;
  let lastDoc = null;
  let totalWrites = 0;

  while (true) {
    let q = firestore.collection(STUDENT_USER_COLL).limit(pageSize);
    if (lastDoc) q = firestore.collection(STUDENT_USER_COLL).startAfter(lastDoc).limit(pageSize);

    const snap = await q.get();
    if (snap.empty) break;

    for (const stu of snap.docs) {
      const s = stu.data();
      const studentId = s?.id || stu.id;       // usa el campo id si existe; de lo contrario, el doc.id
      const nat       = s?.nationality;
      if (!studentId || !nat) continue;

      const previews = candidatesByNat.get(nat);
      if (!previews || previews.length === 0) continue;

      // Resolver perfil en StudentUserPorfile por userId == studentId
      const profSnap = await firestore
        .collection(STUDENT_PROFILE_COLL)
        .where("userId", "==", String(studentId))
        .limit(1)
        .get();

      if (profSnap.empty) continue;
      const profDoc = profSnap.docs[0];

      // Batched upserts (máx 500 por batch; aquí solo 3)
      const batch = firestore.batch();
      for (const p of previews) {
        const recRef = profDoc.ref.collection(REC_SUBCOLL).doc(p.id); // id = housingId
        batch.set(recRef, {
          id: p.id,
          housing: p.housing,
          title: p.title,
          price: p.price,
          rating: p.rating,
          reviewsCount: p.reviewsCount,
          photoPath: p.photoPath,
          source: "nationality_job",
          updatedAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true }); // no borra; upsert
        totalWrites++;
      }
      await batch.commit();
    }

    lastDoc = snap.docs[snap.docs.length - 1];
    if (snap.size < pageSize) break;
  }

  return totalWrites;
}

/** ========= MAIN ========= **/
(async () => {
  console.log(`[JOB] Start | project=${PROJECT_ID} dataset=${BQ_DATASET} windowDays=${DAYS_WINDOW} topTags=${TOP_TAGS_PER_NAT} postsPerNat=${POSTS_PER_NAT}`);

  try {
    const topTagsByNat = await getTopTagsByNationality();
    console.log(`[JOB] topTags nationalities=${topTagsByNat.size}`);
    if (topTagsByNat.size === 0) {
      console.log("[JOB] No tags found. Finishing.");
      process.exit(0);
    }

    const tagCatalog = await loadHousingTagCatalog();
    console.log(`[JOB] tag catalog loaded: ${tagCatalog.nameToId.size} names, ${tagCatalog.idToName.size} ids`);

    const candidatesByNat = await getCandidatePostsByNationality(topTagsByNat, tagCatalog);
    console.log(`[JOB] candidates ready.`);

    const writes = await writeRecommendationsForStudents(candidatesByNat);
    console.log(`[JOB] Done. total upserts=${writes}`);
    process.exit(0);
  } catch (e) {
    console.error("[JOB] ERROR", e);
    process.exit(1);
  }
})();
