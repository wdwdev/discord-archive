import type {
  ChunkDetail,
  Guild,
  Channel,
  ProjectionData,
  SearchResult,
} from "./types";

const BASE = "";

export async function fetchGuilds(): Promise<Guild[]> {
  const res = await fetch(`${BASE}/api/guilds`);
  return res.json();
}

export async function fetchChannels(guildId: string): Promise<Channel[]> {
  const res = await fetch(`${BASE}/api/guilds/${guildId}/channels`);
  return res.json();
}

export async function fetchProjection(guildId: string): Promise<ProjectionData> {
  const res = await fetch(`${BASE}/api/projections/${guildId}`);
  const buffer = await res.arrayBuffer();
  return parseProjection(guildId, buffer);
}

export async function fetchChunk(chunkId: number): Promise<ChunkDetail> {
  const res = await fetch(`${BASE}/api/chunks/${chunkId}`);
  return res.json();
}

export async function searchChunks(
  query: string,
  guildIds?: string[],
  limit = 50,
): Promise<SearchResult[]> {
  const res = await fetch(`${BASE}/api/search`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, guild_ids: guildIds, limit }),
  });
  if (!res.ok) throw new Error(`Search failed: ${res.status}`);
  return res.json();
}

// ---------------------------------------------------------------------------
// Binary parser
// ---------------------------------------------------------------------------

const MAGIC = 0x59584c47; // "GLXY" little-endian: G=0x47 L=0x4C X=0x58 Y=0x59

function parseProjection(guildId: string, buffer: ArrayBuffer): ProjectionData {
  const view = new DataView(buffer);

  const magic = view.getUint32(0, true);
  if (magic !== MAGIC) {
    throw new Error(`Invalid projection file: bad magic 0x${magic.toString(16)}`);
  }

  const version = view.getUint32(4, true);
  if (version !== 1) {
    throw new Error(`Unsupported projection version: ${version}`);
  }

  const numPoints = view.getUint32(8, true);

  const headerSize = 16;
  const positionsBytes = numPoints * 3 * 4;
  const chunkIdsBytes = numPoints * 8;

  const positionsOffset = headerSize;
  const chunkIdsOffset = positionsOffset + positionsBytes;
  const channelIdsOffset = chunkIdsOffset + chunkIdsBytes;

  const positions = new Float32Array(buffer, positionsOffset, numPoints * 3);

  // BigInt64Array requires 8-byte aligned offset; copy if needed
  const chunkIds = chunkIdsOffset % 8 === 0
    ? new BigInt64Array(buffer, chunkIdsOffset, numPoints)
    : new BigInt64Array(buffer.slice(chunkIdsOffset, chunkIdsOffset + chunkIdsBytes));
  const channelIds = channelIdsOffset % 8 === 0
    ? new BigInt64Array(buffer, channelIdsOffset, numPoints)
    : new BigInt64Array(buffer.slice(channelIdsOffset, channelIdsOffset + chunkIdsBytes));

  return { guildId, numPoints, positions, chunkIds, channelIds };
}
