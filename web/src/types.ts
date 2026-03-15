export interface Guild {
  guild_id: string;
  name: string;
  has_projection: boolean;
}

export interface Channel {
  channel_id: string;
  name: string;
  parent_id: string | null;
  type: number;
}

export interface SearchResult {
  chunk_id: number;
  distance: number;
  guild_id: string;
  channel_id: string;
  channel_name: string | null;
  preview: string;
}

export interface ChunkDetail {
  chunk_id: number;
  text: string;
  guild_id: string;
  channel_id: string;
  channel_name: string | null;
  chunk_type: string;
  first_message_at: string | null;
  last_message_at: string | null;
}

/** Parsed binary projection data for one guild. */
export interface ProjectionData {
  guildId: string;
  numPoints: number;
  positions: Float32Array;
  chunkIds: BigInt64Array;
  channelIds: BigInt64Array;
}

export type ColorMode = "guild" | "channel";
