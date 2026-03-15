import { useEffect, useState } from "react";
import { fetchChunk } from "../api";
import type { ChunkDetail } from "../types";

interface Props {
  chunkId: number | null;
}

export default function ChunkPanel({ chunkId }: Props) {
  const [chunk, setChunk] = useState<ChunkDetail | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (chunkId == null) {
      setChunk(null);
      return;
    }

    let cancelled = false;
    setLoading(true);

    fetchChunk(chunkId)
      .then((data) => {
        if (!cancelled) setChunk(data);
      })
      .catch(() => {
        if (!cancelled) setChunk(null);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [chunkId]);

  if (chunkId == null) {
    return (
      <div className="h-full flex items-center justify-center text-white/20 text-sm">
        Click a point to view chunk content
      </div>
    );
  }

  if (loading) {
    return (
      <div className="h-full flex items-center justify-center">
        <div className="w-5 h-5 border-2 border-white/20 border-t-galaxy-accent rounded-full animate-spin" />
      </div>
    );
  }

  if (!chunk) {
    return (
      <div className="h-full flex items-center justify-center text-white/30 text-sm">
        Chunk not found
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="px-3 py-2 border-b border-white/10 flex-shrink-0">
        <div className="flex items-center gap-2 text-xs text-white/40">
          {chunk.channel_name && (
            <span className="text-galaxy-accent">#{chunk.channel_name}</span>
          )}
          <span>{chunk.chunk_type}</span>
          {chunk.first_message_at && (
            <span className="ml-auto">
              {new Date(chunk.first_message_at).toLocaleDateString()}
            </span>
          )}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-3">
        <pre className="text-sm text-white/80 whitespace-pre-wrap font-mono leading-relaxed">
          {chunk.text}
        </pre>
      </div>
    </div>
  );
}
