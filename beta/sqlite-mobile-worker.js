import sqlite3InitModule from "https://cdn.jsdelivr.net/npm/@sqlite.org/sqlite-wasm@3.53.4-build1/dist/index.mjs";

const POOL_NAME = "usse-mobile-beta-sahpool";
const POOL_DIR = "/usse-mobile-beta-sahpool";
let sqlite3 = null;
let poolUtil = null;
let db = null;

function postProgress(percent, text) {
    self.postMessage({ type: "progress", percent, text });
}

function postError(id, error) {
    self.postMessage({
        id,
        ok: false,
        error: error instanceof Error ? (error.message || String(error)) : String(error)
    });
}

function normalizeError(error) {
    return error instanceof Error ? error : new Error(String(error));
}

async function initSQLite() {
    if (poolUtil && sqlite3) return;
    postProgress(10, "Loading mobile SQLite runtime...");
    sqlite3 = await sqlite3InitModule();
    if (!sqlite3 || typeof sqlite3.installOpfsSAHPoolVfs !== "function") {
        throw new Error("This browser does not provide the SQLite OPFS storage backend required by the mobile beta.");
    }
    postProgress(25, "Opening persistent mobile storage...");
    poolUtil = await sqlite3.installOpfsSAHPoolVfs({
        name: POOL_NAME,
        directory: POOL_DIR
    });
}

function makeReplayStream(firstChunk, reader, firstDone) {
    let first = true;
    return new ReadableStream({
        async pull(controller) {
            if (first) {
                first = false;
                if (!firstDone) controller.enqueue(firstChunk);
                if (firstDone) controller.close();
                return;
            }
            const result = await reader.read();
            if (result.done) controller.close();
            else controller.enqueue(result.value);
        },
        cancel(reason) {
            try { reader.cancel(reason); } catch (_) {}
        }
    });
}

async function streamDatabaseIntoOPFS(dbUrl, dbPath) {
    postProgress(35, "Downloading database…");
    const response = await fetch(dbUrl, { cache: "no-store" });
    if (!response.ok) throw new Error(`Database download failed with HTTP ${response.status}.`);
    if (!response.body) throw new Error("This browser did not provide a streaming response body.");

    const reader = response.body.getReader();
    const firstResult = await reader.read();
    if (firstResult.done) throw new Error("Cloudflare returned an empty database payload.");

    const firstChunk = firstResult.value;
    const isGzip = firstChunk.length >= 2 && firstChunk[0] === 0x1f && firstChunk[1] === 0x8b;
    let stream = makeReplayStream(firstChunk, reader, false);

    if (isGzip) {
        if (typeof DecompressionStream === "undefined") {
            throw new Error("This iPhone Safari version does not support streaming gzip decompression. iOS 16.4 or later is required.");
        }
        postProgress(50, "Streaming database decompression…");
        stream = stream.pipeThrough(new DecompressionStream("gzip"));
    } else {
        postProgress(50, "Streaming database into local storage…");
    }

    const decompressedReader = stream.getReader();
    let totalBytes = 0;
    let lastProgressBytes = 0;

    const importedBytes = await poolUtil.importDb(dbPath, async () => {
        const next = await decompressedReader.read();
        if (next.done) return undefined;
        const chunk = next.value;
        totalBytes += chunk.byteLength;
        if (totalBytes - lastProgressBytes >= 32 * 1024 * 1024) {
            lastProgressBytes = totalBytes;
            const progress = 50 + Math.min(44, Math.floor(totalBytes / (16 * 1024 * 1024)));
            postProgress(Math.min(94, progress), `Caching database… ${Math.floor(totalBytes / (1024 * 1024))} MB`);
        }
        return chunk;
    });

    postProgress(95, "Opening local SQLite database…");
    return Number(importedBytes || totalBytes || 0);
}

function getDatabaseRawBytes() {
    const pageCount = Number(db.selectArray("PRAGMA page_count")[0]);
    const pageSize = Number(db.selectArray("PRAGMA page_size")[0]);
    if (!Number.isFinite(pageCount) || !Number.isFinite(pageSize)) return 0;
    return pageCount * pageSize;
}

function openDatabase(dbPath) {
    db = new poolUtil.OpfsSAHPoolDb(dbPath, "c");
    db.exec("PRAGMA query_only = ON;");
    const table = db.selectObject("SELECT name FROM sqlite_master WHERE type='table' AND name='addresses'");
    if (!table) throw new Error("The imported SQLite database does not contain the addresses table.");
    return getDatabaseRawBytes();
}

async function handleOpen(id, payload) {
    await initSQLite();
    const dbPath = payload.dbPath || "/PRN_Data.db";
    const expectedRawBytes = Number(payload.expectedRawBytes) || 0;
    const expectedRowCount = Number(payload.expectedRowCount) || 0;
    const expectedVersion = payload.expectedVersion || "";
    const cachedVersion = payload.cachedVersion || "";

    let fromCache = false;
    let fileExists = false;
    try {
        const files = poolUtil.getFileNames();
        fileExists = Array.isArray(files) && files.includes(dbPath);
    } catch (_) {}

    const needsImport = !fileExists || !cachedVersion || !expectedVersion || cachedVersion !== expectedVersion;

    if (db) {
        try { db.close(); } catch (_) {}
        db = null;
    }

    if (needsImport) {
        try {
            await streamDatabaseIntoOPFS(payload.dbUrl, dbPath);
        } catch (err) {
            throw normalizeError(err);
        }
    } else {
        postProgress(85, "Loading database from iPhone storage…");
        fromCache = true;
    }

    const rawBytes = openDatabase(dbPath);
    if (expectedRawBytes && rawBytes !== expectedRawBytes) {
        try { db.close(); } catch (_) {}
        db = null;
        throw new Error(`Mobile database size mismatch: expected ${expectedRawBytes.toLocaleString()} bytes, received ${rawBytes.toLocaleString()} bytes.`);
    }

    let actualRowCount = null;
    if (needsImport && expectedRowCount) {
        actualRowCount = Number(db.selectArray("SELECT COUNT(*) FROM addresses")[0]);
        if (actualRowCount !== expectedRowCount) {
            try { db.close(); } catch (_) {}
            db = null;
            throw new Error(`Mobile database row-count mismatch: expected ${expectedRowCount.toLocaleString()}, received ${actualRowCount.toLocaleString()}.`);
        }
    }

    return {
        ready: true,
        fromCache,
        rawBytes,
        version: expectedVersion || cachedVersion || null,
        rowCount: actualRowCount ?? (expectedRowCount || null),
        vfs: POOL_NAME
    };
}

async function handleQuery(id, payload) {
    if (!db) throw new Error("Mobile SQLite database is not open.");
    const sql = String(payload.sql || "").trim();
    if (!sql) return [];
    const params = Array.isArray(payload.params) ? payload.params : [];
    return db.exec({
        sql,
        bind: params,
        rowMode: "object",
        returnValue: "resultRows"
    });
}

self.onmessage = async (event) => {
    const msg = event.data || {};
    const id = msg.id;
    try {
        if (msg.type === "open") {
            const result = await handleOpen(id, msg);
            self.postMessage({ id, ok: true, result });
        } else if (msg.type === "query") {
            const result = await handleQuery(id, msg);
            self.postMessage({ id, ok: true, result });
        } else {
            throw new Error(`Unknown mobile SQLite operation: ${msg.type}`);
        }
    } catch (err) {
        postError(id, normalizeError(err));
    }
};
