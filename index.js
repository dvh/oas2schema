import { mkdir, rm, writeFile } from "node:fs/promises";
import { join, relative, dirname } from "node:path";
import { createHash } from "node:crypto";
import { upgrade } from "@scalar/openapi-upgrader";

const API_BASE = "https://api.developer.overheid.nl/api-register/v1";
const API_KEY = "X";
const REGISTER_DIR = "./register";
const PER_PAGE = 100;
const OAS_VERSION = "3.0.json";
const CONCURRENCY = 5;
const SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema";
const ID_BASE = "https://schemas.don.apps.digilab.network";

const EXCLUDE_HAL = !process.argv.includes("--include-hal");

const ALWAYS_SKIP = /^(allOf\d|oneOf\d|anyOf\d)/;
const HAL_SKIP = /^(_embedded|_links|Hal[A-Z])/;
const HAL_SKIP_EXACT = /^(Self\d*|SelfLinks\d*|Href\d*|Links?\d*)$/;
function isSkippedSchema(name) {
  if (ALWAYS_SKIP.test(name)) return true;
  if (EXCLUDE_HAL && (HAL_SKIP.test(name) || HAL_SKIP_EXACT.test(name))) return true;
  return false;
}

/** Convert a property name to PascalCase for use as a schema name */
function pascalCase(str) {
  return str
    .replace(/(^|[_-])([a-z])/g, (_, _sep, ch) => ch.toUpperCase())
    .replace(/^[a-z]/, (ch) => ch.toUpperCase());
}

// ── Utility functions ──────────────────────────────────────────────────

function stableStringify(obj) {
  if (obj === null || typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return "[" + obj.map(stableStringify).join(",") + "]";
  const keys = Object.keys(obj).sort();
  return (
    "{" +
    keys.map((k) => JSON.stringify(k) + ":" + stableStringify(obj[k])).join(",") +
    "}"
  );
}

function contentHash(schema) {
  return createHash("sha256").update(stableStringify(schema)).digest("hex");
}

/** Hash ignoring $ref values — for dedup where refs are internal identifiers, not content */
function structuralHash(schema) {
  const stripped = stableStringify(schema).replace(/"\$ref":"[^"]*"/g, '"$ref":""');
  return createHash("sha256").update(stripped).digest("hex");
}

function slug(str) {
  return str
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/([a-z])([A-Z])/g, "$1-$2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1-$2")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

function orgSlug(label) {
  return slug(label);
}

function apiDirName(title, id) {
  return `${slug(title)}-${id.toLowerCase()}`;
}

/** Schema filename: keep original CamelCase name, add .schema.json */
function schemaFileName(name) {
  return `${name}.schema.json`;
}

/** Kebab-case slug for $id URLs */
function schemaSlug(name) {
  return slug(name);
}


/** Recursively sort `properties` keys alphabetically */
function sortProperties(schema) {
  if (schema === null || typeof schema !== "object") return schema;
  if (Array.isArray(schema)) return schema.map(sortProperties);

  const result = {};
  for (const [key, value] of Object.entries(schema)) {
    if (key === "properties" && value && typeof value === "object" && !Array.isArray(value)) {
      const sorted = {};
      for (const k of Object.keys(value).sort()) {
        sorted[k] = sortProperties(value[k]);
      }
      result[key] = sorted;
    } else {
      result[key] = sortProperties(value);
    }
  }
  return result;
}

// ── API fetching ───────────────────────────────────────────────────────

async function apiFetch(path) {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, { headers: { "X-Api-Key": API_KEY } });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} — ${url}`);
  return { body: await res.json(), headers: res.headers };
}

async function fetchAllApis() {
  const apis = [];
  let page = 1;
  let totalPages = 1;
  while (page <= totalPages) {
    console.log(`Fetching API list page ${page}/${totalPages}…`);
    const { body, headers } = await apiFetch(
      `/apis?page=${page}&perPage=${PER_PAGE}`
    );
    apis.push(...body);
    const tp = headers.get("Total-Pages");
    if (tp) totalPages = parseInt(tp, 10);
    page++;
  }
  console.log(`Found ${apis.length} APIs.\n`);
  return apis;
}

async function runWithConcurrency(items, fn, concurrency) {
  const results = [];
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await fn(items[idx]);
    }
  }
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return results;
}

// ── Phase 2: Inline all $refs ──────────────────────────────────────────

function resolveJsonPointer(obj, pointer) {
  const parts = pointer.split("/").filter(Boolean);
  let current = obj;
  for (const part of parts) {
    const key = part.replace(/~1/g, "/").replace(/~0/g, "~");
    if (current === null || typeof current !== "object") return undefined;
    if (Array.isArray(current)) {
      const idx = parseInt(key, 10);
      current = current[idx];
    } else {
      current = current[key];
    }
  }
  return current;
}

function inlineRefs(schema, allSchemas, resolving = new Set()) {
  if (schema === null || typeof schema !== "object") return schema;
  if (Array.isArray(schema))
    return schema.map((item) => inlineRefs(item, allSchemas, resolving));

  if (
    typeof schema.$ref === "string" &&
    schema.$ref.startsWith("#/components/schemas/")
  ) {
    const rest = schema.$ref.slice("#/components/schemas/".length);
    const slashIdx = rest.indexOf("/");
    const name = slashIdx === -1 ? rest : rest.slice(0, slashIdx);
    const subPath = slashIdx === -1 ? null : rest.slice(slashIdx);

    if (resolving.has(name)) {
      // For sub-path refs into a circular schema, resolve the pointer on the
      // original (non-inlined) schema — we're extracting a fragment, not
      // recursively inlining the whole thing.
      if (subPath) {
        const target = allSchemas[name];
        if (target) {
          const resolved = resolveJsonPointer(target, subPath);
          if (resolved !== undefined) {
            return inlineRefs(structuredClone(resolved), allSchemas, resolving);
          }
        }
      }
      return schema;
    }
    const target = allSchemas[name];
    if (!target) return schema;

    resolving.add(name);
    const inlined = inlineRefs(structuredClone(target), allSchemas, resolving);
    resolving.delete(name);

    if (subPath) {
      const resolved = resolveJsonPointer(inlined, subPath);
      return resolved !== undefined ? structuredClone(resolved) : schema;
    }
    return inlined;
  }

  const result = {};
  for (const [key, value] of Object.entries(schema)) {
    result[key] = inlineRefs(value, allSchemas, resolving);
  }
  return result;
}

// ── Phase 3: Extract inline objects ────────────────────────────────────

function isExtractableObject(schema) {
  return (
    schema &&
    typeof schema === "object" &&
    !Array.isArray(schema) &&
    schema.type === "object" &&
    schema.properties &&
    !schema.$ref
  );
}

function registerExtracted(schema, suggestedName, hashToName, allSchemas) {
  const hash = contentHash(schema);
  if (hashToName.has(hash)) {
    const existing = hashToName.get(hash);
    if (isSkippedSchema(existing)) return null;
    return existing;
  }

  let name = suggestedName;
  let counter = 2;
  while (allSchemas[name] && contentHash(allSchemas[name]) !== hash) {
    name = `${suggestedName}${counter}`;
    counter++;
  }

  // Don't extract schemas matching skip pattern — keep them inline
  if (isSkippedSchema(name)) return null;

  hashToName.set(hash, name);
  allSchemas[name] = schema;
  return name;
}

function extractInlineObjects(schema, hashToName, allSchemas) {
  if (schema === null || typeof schema !== "object") return schema;
  if (Array.isArray(schema))
    return schema.map((item) =>
      extractInlineObjects(item, hashToName, allSchemas)
    );
  if (schema.$ref) return schema;

  const result = { ...schema };

  if (result.properties && typeof result.properties === "object") {
    const newProps = {};
    for (const [propName, propSchema] of Object.entries(result.properties)) {
      let processed = extractInlineObjects(propSchema, hashToName, allSchemas);
      if (isExtractableObject(processed)) {
        const name = registerExtracted(
          processed,
          processed.title || pascalCase(propName),
          hashToName,
          allSchemas
        );
        newProps[propName] = name
          ? { $ref: `#/components/schemas/${name}` }
          : processed;
      } else {
        newProps[propName] = processed;
      }
    }
    result.properties = newProps;
  }

  if (result.items) {
    let processed = extractInlineObjects(result.items, hashToName, allSchemas);
    if (isExtractableObject(processed)) {
      const name = registerExtracted(
        processed,
        processed.title || "Item",
        hashToName,
        allSchemas
      );
      result.items = name
        ? { $ref: `#/components/schemas/${name}` }
        : processed;
    } else {
      result.items = processed;
    }
  }

  for (const keyword of ["allOf", "oneOf", "anyOf"]) {
    if (Array.isArray(result[keyword])) {
      result[keyword] = result[keyword].map((member, i) => {
        let processed = extractInlineObjects(member, hashToName, allSchemas);
        if (isExtractableObject(processed)) {
          const name = registerExtracted(
            processed,
            processed.title || `${keyword}${i}`,
            hashToName,
            allSchemas
          );
          return name
            ? { $ref: `#/components/schemas/${name}` }
            : processed;
        }
        return processed;
      });
    }
  }

  if (
    result.additionalProperties &&
    typeof result.additionalProperties === "object" &&
    !Array.isArray(result.additionalProperties)
  ) {
    let processed = extractInlineObjects(
      result.additionalProperties,
      hashToName,
      allSchemas
    );
    if (isExtractableObject(processed)) {
      const name = registerExtracted(
        processed,
        processed.title || "AdditionalProperties",
        hashToName,
        allSchemas
      );
      result.additionalProperties = name
        ? { $ref: `#/components/schemas/${name}` }
        : processed;
    } else {
      result.additionalProperties = processed;
    }
  }

  return result;
}

// ── $ref rewriting ─────────────────────────────────────────────────────

function rewriteSchemaRef(value, refMap, currentSchemaName) {
  if (typeof value !== "string" || !value.startsWith("#/components/schemas/"))
    return value;
  const rest = value.slice("#/components/schemas/".length);
  const slashIdx = rest.indexOf("/");
  const name = slashIdx === -1 ? rest : rest.slice(0, slashIdx);
  const fragment = slashIdx === -1 ? "" : rest.slice(slashIdx);

  if (!refMap[name]) return value;
  if (name === currentSchemaName && fragment) return `#${fragment}`;
  if (fragment) return `${refMap[name]}#${fragment}`;
  return refMap[name];
}

function rewriteRefs(schema, refMap, currentSchemaName) {
  if (schema === null || typeof schema !== "object") return schema;
  if (Array.isArray(schema))
    return schema.map((item) => rewriteRefs(item, refMap, currentSchemaName));

  const result = {};
  for (const [key, value] of Object.entries(schema)) {
    if (key === "$ref" && typeof value === "string") {
      result["$ref"] = rewriteSchemaRef(value, refMap, currentSchemaName);
    } else if (
      key === "discriminator" &&
      value &&
      typeof value === "object" &&
      value.mapping
    ) {
      const newMapping = {};
      for (const [mk, mv] of Object.entries(value.mapping)) {
        newMapping[mk] = rewriteSchemaRef(mv, refMap, currentSchemaName);
      }
      result[key] = rewriteRefs(
        { ...value, mapping: newMapping },
        refMap,
        currentSchemaName
      );
    } else {
      result[key] = rewriteRefs(value, refMap, currentSchemaName);
    }
  }
  return result;
}

function findRefs(obj) {
  const refs = new Set();
  function addRef(value) {
    if (
      typeof value === "string" &&
      value.startsWith("#/components/schemas/")
    ) {
      const rest = value.slice("#/components/schemas/".length);
      const slashIdx = rest.indexOf("/");
      refs.add(slashIdx === -1 ? rest : rest.slice(0, slashIdx));
    }
  }
  function walk(node) {
    if (node === null || typeof node !== "object") return;
    if (Array.isArray(node)) {
      node.forEach(walk);
      return;
    }
    for (const [key, value] of Object.entries(node)) {
      if (key === "$ref") addRef(value);
      else if (
        key === "discriminator" &&
        value &&
        typeof value === "object" &&
        value.mapping
      ) {
        for (const mv of Object.values(value.mapping)) addRef(mv);
        walk(value);
      } else walk(value);
    }
  }
  walk(obj);
  return refs;
}

// ── Post-extraction ref rewriting ───────────────────────────────────────

function rewriteMergedRef(value, renameMap) {
  if (typeof value !== "string" || !value.startsWith("#/components/schemas/"))
    return value;
  const rest = value.slice("#/components/schemas/".length);
  const slashIdx = rest.indexOf("/");
  const name = slashIdx === -1 ? rest : rest.slice(0, slashIdx);
  const suffix = slashIdx === -1 ? "" : rest.slice(slashIdx);
  if (renameMap.has(name))
    return `#/components/schemas/${renameMap.get(name)}${suffix}`;
  return value;
}

function rewriteMergedRefs(schema, renameMap) {
  if (schema === null || typeof schema !== "object") return schema;
  if (Array.isArray(schema))
    return schema.map((item) => rewriteMergedRefs(item, renameMap));

  const result = {};
  for (const [key, value] of Object.entries(schema)) {
    if (key === "$ref" && typeof value === "string") {
      result["$ref"] = rewriteMergedRef(value, renameMap);
    } else if (
      key === "discriminator" &&
      value &&
      typeof value === "object" &&
      value.mapping
    ) {
      const newMapping = {};
      for (const [mk, mv] of Object.entries(value.mapping)) {
        newMapping[mk] = rewriteMergedRef(mv, renameMap);
      }
      result[key] = rewriteMergedRefs(
        { ...value, mapping: newMapping },
        renameMap
      );
    } else {
      result[key] = rewriteMergedRefs(value, renameMap);
    }
  }
  return result;
}

// ── Strip dangling refs ─────────────────────────────────────────────────

function stripDanglingRefs(schema, allSchemas) {
  if (schema === null || typeof schema !== "object") return schema;
  if (Array.isArray(schema))
    return schema.map((item) => stripDanglingRefs(item, allSchemas));

  if (
    typeof schema.$ref === "string" &&
    schema.$ref.startsWith("#/components/schemas/")
  ) {
    const rest = schema.$ref.slice("#/components/schemas/".length);
    const name = rest.includes("/") ? rest.slice(0, rest.indexOf("/")) : rest;
    if (!allSchemas[name]) {
      // Remove the dangling $ref — return empty schema (matches anything)
      return {};
    }
  }

  const result = {};
  for (const [key, value] of Object.entries(schema)) {
    if (
      key === "discriminator" &&
      value &&
      typeof value === "object" &&
      value.mapping
    ) {
      // Strip dangling mapping entries
      const newMapping = {};
      for (const [mk, mv] of Object.entries(value.mapping)) {
        if (typeof mv === "string" && mv.startsWith("#/components/schemas/")) {
          const rest = mv.slice("#/components/schemas/".length);
          const name = rest.includes("/") ? rest.slice(0, rest.indexOf("/")) : rest;
          if (allSchemas[name]) newMapping[mk] = mv;
        } else {
          newMapping[mk] = mv;
        }
      }
      if (Object.keys(newMapping).length > 0) {
        result[key] = stripDanglingRefs({ ...value, mapping: newMapping }, allSchemas);
      } else {
        // Drop discriminator entirely if no valid mappings
        result[key] = stripDanglingRefs({ ...value, mapping: undefined }, allSchemas);
      }
    } else {
      result[key] = stripDanglingRefs(value, allSchemas);
    }
  }

  // Clean up allOf/oneOf/anyOf arrays that may contain empty objects from stripped refs
  for (const keyword of ["allOf", "oneOf", "anyOf"]) {
    if (Array.isArray(result[keyword])) {
      result[keyword] = result[keyword].filter(
        (item) => !(typeof item === "object" && Object.keys(item).length === 0)
      );
      if (result[keyword].length === 0) delete result[keyword];
      else if (result[keyword].length === 1 && keyword === "allOf") {
        // Unwrap single-element allOf
        Object.assign(result, result[keyword][0]);
        delete result[keyword];
      }
    }
  }

  return result;
}

// ── Circular reference detection ────────────────────────────────────────

function findCircularSchemas(allSchemas) {
  // Build adjacency: schema name → Set of schema names it references
  const adj = new Map();
  for (const [name, schema] of Object.entries(allSchemas)) {
    const refs = findRefs(schema);
    // Only keep refs to schemas that exist in allSchemas
    const valid = new Set();
    for (const r of refs) {
      if (allSchemas[r]) valid.add(r);
    }
    adj.set(name, valid);
  }

  // Tarjan's algorithm to find all SCCs (strongly connected components)
  let index = 0;
  const stack = [];
  const onStack = new Set();
  const indices = new Map();
  const lowlinks = new Map();
  const circular = new Set();

  function strongconnect(v) {
    indices.set(v, index);
    lowlinks.set(v, index);
    index++;
    stack.push(v);
    onStack.add(v);

    for (const w of adj.get(v) || []) {
      if (!indices.has(w)) {
        strongconnect(w);
        lowlinks.set(v, Math.min(lowlinks.get(v), lowlinks.get(w)));
      } else if (onStack.has(w)) {
        lowlinks.set(v, Math.min(lowlinks.get(v), indices.get(w)));
      }
    }

    if (lowlinks.get(v) === indices.get(v)) {
      const scc = [];
      let w;
      do {
        w = stack.pop();
        onStack.delete(w);
        scc.push(w);
      } while (w !== v);
      // SCC with size > 1 means a cycle; size 1 with self-ref also counts
      if (scc.length > 1) {
        for (const n of scc) circular.add(n);
      } else if (adj.get(scc[0])?.has(scc[0])) {
        circular.add(scc[0]);
      }
    }
  }

  for (const name of adj.keys()) {
    if (!indices.has(name)) strongconnect(name);
  }

  return circular;
}

// ── Main ───────────────────────────────────────────────────────────────

async function main() {
  // ── Phase 1: Fetch ──
  const apis = await fetchAllApis();
  console.log("Fetching OAS specs…\n");

  const apiData = [];
  const skipped = [];

  await runWithConcurrency(
    apis,
    async (api) => {
      const label = `${api.title} (${api.id})`;
      let oas;
      try {
        ({ body: oas } = await apiFetch(`/apis/${api.id}/oas/${OAS_VERSION}`));
      } catch (err) {
        console.warn(`  ⚠ Skipping ${label}: ${err.message}`);
        skipped.push({ id: api.id, title: api.title, error: err.message });
        return;
      }

      // Upgrade OAS 3.0 → 3.1+ (schemas become JSON Schema 2020-12)
      const upgraded = upgrade(oas);
      const allRawSchemas = upgraded.components?.schemas ?? upgraded.definitions ?? {};
      const rawSchemas = {};
      for (const [name, schema] of Object.entries(allRawSchemas)) {
        if (!isSkippedSchema(name)) rawSchemas[name] = schema;
      }
      const rawCount = Object.keys(rawSchemas).length;
      if (rawCount === 0) {
        console.log(`  – ${label}: no schemas`);
        return;
      }

      // Phase 2: Inline all $refs
      const inlinedSchemas = {};
      for (const [name, schema] of Object.entries(rawSchemas)) {
        inlinedSchemas[name] = inlineRefs(schema, rawSchemas);
      }

      // PascalCase all schema names
      const nameMap = new Map();
      for (const name of Object.keys(inlinedSchemas)) {
        const pc = pascalCase(name);
        if (pc !== name) nameMap.set(name, pc);
      }
      const jsonSchemas = {};
      for (const [name, schema] of Object.entries(inlinedSchemas)) {
        const pcName = nameMap.get(name) || name;
        jsonSchemas[pcName] = nameMap.size > 0
          ? rewriteMergedRefs(schema, nameMap)
          : schema;
      }

      // Phase 3: Extract inline objects
      const allSchemas = { ...jsonSchemas };
      const hashToName = new Map();
      for (const name of Object.keys(jsonSchemas)) {
        allSchemas[name] = extractInlineObjects(
          allSchemas[name],
          hashToName,
          allSchemas
        );
      }

      // Post-extraction dedup: iteratively merge schemas with identical content.
      // Rewriting refs after a merge can make previously-different schemas identical,
      // so repeat until stable.
      let merged;
      do {
        merged = 0;
        const postHashToCanonical = new Map();
        const renameMap = new Map();
        for (const name of Object.keys(allSchemas)) {
          const h = structuralHash(allSchemas[name]);
          if (postHashToCanonical.has(h)) {
            const canonical = postHashToCanonical.get(h);
            renameMap.set(name, canonical);
            delete allSchemas[name];
            merged++;
          } else {
            postHashToCanonical.set(h, name);
          }
        }
        if (renameMap.size > 0) {
          for (const name of Object.keys(allSchemas)) {
            allSchemas[name] = rewriteMergedRefs(allSchemas[name], renameMap);
          }
        }
      } while (merged > 0);

      // Remove extracted schemas matching skip pattern
      for (const name of Object.keys(allSchemas)) {
        if (isSkippedSchema(name)) delete allSchemas[name];
      }

      // Remove schemas involved in circular references
      const circularNames = findCircularSchemas(allSchemas);
      if (circularNames.size > 0) {
        for (const name of circularNames) delete allSchemas[name];
      }

      // Strip dangling $refs pointing to removed schemas
      for (const name of Object.keys(allSchemas)) {
        allSchemas[name] = stripDanglingRefs(allSchemas[name], allSchemas);
      }

      const totalCount = Object.keys(allSchemas).length;
      const extracted = totalCount - rawCount;
      console.log(
        `  ✓ ${label}: ${rawCount} schemas${extracted > 0 ? ` (+${extracted} extracted)` : ""}`
      );

      // Track original OAS schema names (before PascalCase and extraction)
      const originalNames = new Set(Object.keys(rawSchemas));
      apiData.push({ api, schemas: allSchemas, originalNames });
    },
    CONCURRENCY
  );

  // ── Phase 4: Hash & classify ──
  console.log("\nClassifying schemas…");

  const allEntries = [];
  for (const { api, schemas, originalNames } of apiData) {
    const org = api.organisation ?? {};
    const oSlug = orgSlug(org.label ?? "unknown");

    for (const [name, schema] of Object.entries(schemas)) {
      // Find original OAS name: check if the name (or its pre-PascalCase form) was in the OAS
      let oasName = null;
      if (originalNames.has(name)) {
        oasName = name;
      } else {
        // Check if lowercase version matches any original
        for (const orig of originalNames) {
          if (pascalCase(orig) === name) { oasName = orig; break; }
        }
      }

      allEntries.push({
        hash: contentHash(schema),
        orgSlug: oSlug,
        orgLabel: org.label ?? "Unknown",
        orgUri: org.uri ?? "",
        apiId: api.id,
        apiTitle: api.title,
        apiOasUrl: api.oasUrl ?? "",
        apiLifecycle: api.lifecycle ?? {},
        schemaName: name,
        oasName, // null if extracted (not from original OAS)
        schema,
      });
    }
  }

  const hashToApis = new Map();
  const hashToOrgs = new Map();
  const hashToEntries = new Map();

  for (const entry of allEntries) {
    if (!hashToApis.has(entry.hash)) hashToApis.set(entry.hash, new Set());
    hashToApis.get(entry.hash).add(entry.apiId);
    if (!hashToOrgs.has(entry.hash)) hashToOrgs.set(entry.hash, new Set());
    hashToOrgs.get(entry.hash).add(entry.orgSlug);
    if (!hashToEntries.has(entry.hash)) hashToEntries.set(entry.hash, []);
    hashToEntries.get(entry.hash).push(entry);
  }

  // Three tiers: shared (2+ orgs), org (2+ APIs in 1 org), api (1 API)
  const hashLevel = new Map();
  for (const [hash, orgSet] of hashToOrgs) {
    if (orgSet.size >= 2) {
      hashLevel.set(hash, "shared");
    } else if (hashToApis.get(hash).size >= 2) {
      hashLevel.set(hash, "org");
    } else {
      hashLevel.set(hash, "api");
    }
  }

  // ── Phase 5: Determine file locations ──
  console.log("Determining file locations…");

  // For shared schemas: pick canonical CamelCase name, handle collisions
  const sharedNameCandidates = new Map(); // name → [{hash, count}]
  for (const [hash, level] of hashLevel) {
    if (level !== "shared") continue;
    const entries = hashToEntries.get(hash);
    // Pick the most common original schema name
    const nameCounts = new Map();
    for (const e of entries) {
      nameCounts.set(e.schemaName, (nameCounts.get(e.schemaName) || 0) + 1);
    }
    let bestName = "";
    let bestCount = 0;
    for (const [n, c] of nameCounts) {
      if (c > bestCount) {
        bestName = n;
        bestCount = c;
      }
    }
    if (!sharedNameCandidates.has(bestName))
      sharedNameCandidates.set(bestName, []);
    sharedNameCandidates.get(bestName).push({ hash, count: entries.length });
  }

  const sharedHashToName = new Map();
  for (const [name, candidates] of sharedNameCandidates) {
    candidates.sort((a, b) => b.count - a.count);
    sharedHashToName.set(candidates[0].hash, name);
    for (let i = 1; i < candidates.length; i++) {
      hashLevel.set(candidates[i].hash, "api");
    }
  }

  // For org-shared schemas: pick canonical name per org, handle collisions
  const orgSharedNameCandidates = new Map(); // orgSlug → Map<name, [{hash, count}]>
  for (const [hash, level] of hashLevel) {
    if (level !== "org") continue;
    const entries = hashToEntries.get(hash);
    const oSlug = entries[0].orgSlug;

    if (!orgSharedNameCandidates.has(oSlug))
      orgSharedNameCandidates.set(oSlug, new Map());
    const orgMap = orgSharedNameCandidates.get(oSlug);

    const nameCounts = new Map();
    for (const e of entries) {
      nameCounts.set(e.schemaName, (nameCounts.get(e.schemaName) || 0) + 1);
    }
    let bestName = "";
    let bestCount = 0;
    for (const [n, c] of nameCounts) {
      if (c > bestCount) {
        bestName = n;
        bestCount = c;
      }
    }
    if (!orgMap.has(bestName)) orgMap.set(bestName, []);
    orgMap.get(bestName).push({ hash, count: entries.length });
  }

  const orgSharedHashToName = new Map(); // hash → name
  const orgSharedHashToOrg = new Map(); // hash → orgSlug
  for (const [oSlug, nameMap] of orgSharedNameCandidates) {
    for (const [name, candidates] of nameMap) {
      candidates.sort((a, b) => b.count - a.count);
      orgSharedHashToName.set(candidates[0].hash, name);
      orgSharedHashToOrg.set(candidates[0].hash, oSlug);
      for (let i = 1; i < candidates.length; i++) {
        hashLevel.set(candidates[i].hash, "api");
      }
    }
  }

  // Build per-entry output path (relative to REGISTER_DIR)
  const entryOutputPath = new Map();

  for (const entry of allEntries) {
    const level = hashLevel.get(entry.hash);
    const apiDir = apiDirName(entry.apiTitle, entry.apiId);
    const apiDirPath = join(entry.orgSlug, apiDir);

    let outputPath;
    if (level === "shared" && sharedHashToName.has(entry.hash)) {
      const name = sharedHashToName.get(entry.hash);
      outputPath = join("_shared", schemaFileName(name));
    } else if (level === "org" && orgSharedHashToName.has(entry.hash)) {
      const name = orgSharedHashToName.get(entry.hash);
      outputPath = join(entry.orgSlug, "_shared", schemaFileName(name));
    } else {
      outputPath = join(apiDirPath, schemaFileName(entry.schemaName));
    }

    entryOutputPath.set(`${entry.apiId}:${entry.schemaName}`, outputPath);
  }

  // Handle case-insensitive filename collisions within each directory.
  // Build a map of dir → lowerBase → [{key, baseName, hash}] for all entries.
  const dirFiles = new Map();
  for (const entry of allEntries) {
    const key = `${entry.apiId}:${entry.schemaName}`;
    const outputPath = entryOutputPath.get(key);
    const dir = dirname(outputPath);
    const baseName = outputPath.slice(dir.length + 1, -".schema.json".length);
    const lowerBase = baseName.toLowerCase();

    if (!dirFiles.has(dir)) dirFiles.set(dir, new Map());
    const dirMap = dirFiles.get(dir);
    if (!dirMap.has(lowerBase)) dirMap.set(lowerBase, []);
    dirMap.get(lowerBase).push({ key, baseName, hash: entry.hash });
  }

  for (const [dir, dirMap] of dirFiles) {
    const reserved = new Set([...dirMap.keys()]);
    for (const [lowerBase, group] of dirMap) {
      // Find distinct hashes in this group
      const uniqueHashes = [...new Set(group.map((g) => g.hash))];
      if (uniqueHashes.length <= 1) {
        // Same content (or only one entry) — unify all to the first name
        const canonicalName = group[0].baseName;
        const canonicalPath = join(dir, schemaFileName(canonicalName));
        for (const { key } of group) {
          entryOutputPath.set(key, canonicalPath);
        }
        continue;
      }
      // Multiple distinct schemas with case-colliding names:
      // keep the first hash's name, rename the rest
      const firstHash = uniqueHashes[0];
      for (let i = 1; i < uniqueHashes.length; i++) {
        const h = uniqueHashes[i];
        const representative = group.find((g) => g.hash === h);
        let suffix = 2;
        while (reserved.has(`${representative.baseName.toLowerCase()}${suffix}`))
          suffix++;
        const newName = `${representative.baseName}${suffix}`;
        reserved.add(newName.toLowerCase());
        const newPath = join(dir, schemaFileName(newName));
        for (const { key, hash } of group) {
          if (hash === h) entryOutputPath.set(key, newPath);
        }
      }
    }
  }

  // Build refMap per API: schemaName → relative path from API dir
  const apiRefMaps = new Map();

  for (const entry of allEntries) {
    if (!apiRefMaps.has(entry.apiId))
      apiRefMaps.set(entry.apiId, new Map());
    const refMap = apiRefMaps.get(entry.apiId);
    if (refMap.has(entry.schemaName)) continue;

    const key = `${entry.apiId}:${entry.schemaName}`;
    const outputPath = entryOutputPath.get(key);
    const apiDir = apiDirName(entry.apiTitle, entry.apiId);
    const apiDirPath = join(entry.orgSlug, apiDir);
    const rel = relative(apiDirPath, outputPath);
    refMap.set(entry.schemaName, rel.startsWith(".") ? rel : `./${rel}`);
  }

  // ── Phase 6: Write output ──
  console.log("Writing register…\n");
  await rm(REGISTER_DIR, { recursive: true, force: true });
  await mkdir(REGISTER_DIR, { recursive: true });

  const written = new Set();
  let sharedCount = 0;
  let orgSharedCount = 0;
  let apiSpecificCount = 0;

  for (const entry of allEntries) {
    const key = `${entry.apiId}:${entry.schemaName}`;
    const outputPath = entryOutputPath.get(key);
    if (written.has(outputPath)) continue;
    written.add(outputPath);

    const level = hashLevel.get(entry.hash);
    let schemaRefMap;

    if (level === "shared" || level === "org") {
      schemaRefMap = {};
      const schemaDir = dirname(outputPath);
      const refs = findRefs(entry.schema);
      for (const refName of refs) {
        const entries = hashToEntries.get(entry.hash);
        for (const e of entries) {
          const eRefMap = apiRefMaps.get(e.apiId);
          if (eRefMap && eRefMap.has(refName)) {
            const targetPath = entryOutputPath.get(
              `${e.apiId}:${refName}`
            );
            if (targetPath) {
              const rel = relative(schemaDir, targetPath);
              schemaRefMap[refName] = rel.startsWith(".")
                ? rel
                : `./${rel}`;
              break;
            }
          }
        }
      }
    } else {
      schemaRefMap = {};
      const schemaDir = dirname(outputPath);
      const apiDir = apiDirName(entry.apiTitle, entry.apiId);
      const apiDirPath = join(entry.orgSlug, apiDir);
      const refMapForApi = apiRefMaps.get(entry.apiId);
      if (refMapForApi) {
        for (const [name, relFromApi] of refMapForApi) {
          const absTarget = join(apiDirPath, relFromApi);
          const rel = relative(schemaDir, absTarget);
          schemaRefMap[name] = rel.startsWith(".") ? rel : `./${rel}`;
        }
      }
    }

    const rewritten = rewriteRefs(entry.schema, schemaRefMap, entry.schemaName);

    // Build kebab-case $id from output filename
    const idDir = dirname(outputPath);
    const baseName = outputPath.slice(idDir.length + 1, -".schema.json".length);
    const idPath = join(idDir, baseName.toLowerCase());

    // Collect OAS origins
    const origins = hashToEntries.get(entry.hash)
      .map((e) => e.oasName
        ? `${e.apiOasUrl}#/components/schemas/${e.oasName}`
        : e.apiOasUrl)
      .filter((v, i, a) => a.indexOf(v) === i)
      .sort();

    // Add $schema and $id
    const schemaWithMeta = sortProperties({
      $schema: SCHEMA_DIALECT,
      $id: `${ID_BASE}/${idPath}`,
      "x-derived-from": origins,
      ...rewritten,
    });

    const fullPath = join(REGISTER_DIR, outputPath);
    await mkdir(dirname(fullPath), { recursive: true });
    await writeFile(fullPath, JSON.stringify(schemaWithMeta, null, 2) + "\n");

    if (level === "shared") sharedCount++;
    else if (level === "org") orgSharedCount++;
    else apiSpecificCount++;
  }

  // ── Summary ──
  console.log("=== Summary ===");
  console.log(`APIs processed: ${apis.length}`);
  console.log(`APIs with schemas: ${apiData.length}`);
  console.log(`APIs skipped: ${skipped.length}`);
  console.log(`Total schema entries: ${allEntries.length}`);
  console.log(`Unique schema files written: ${written.size}`);
  console.log(`  Shared (cross-org): ${sharedCount}`);
  console.log(`  Org-shared: ${orgSharedCount}`);
  console.log(`  API-specific: ${apiSpecificCount}`);
  console.log(`Organisations: ${new Set(allEntries.map((e) => e.orgSlug)).size}`);
  console.log(`Output: ${REGISTER_DIR}/`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
