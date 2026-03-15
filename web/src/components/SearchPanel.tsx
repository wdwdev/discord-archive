import { useState, useCallback, useRef } from "react";
import { searchChunks } from "../api";
import type { SearchResult } from "../types";

interface Props {
  activeGuildIds: string[];
  onResults: (results: SearchResult[]) => void;
  onSelectResult: (chunkId: number) => void;
  results: SearchResult[];
  selectedChunkId: number | null;
}

export default function SearchPanel({
  activeGuildIds,
  onResults,
  onSelectResult,
  results,
  selectedChunkId,
}: Props) {
  const [query, setQuery] = useState("");
  const [isSearching, setIsSearching] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const doSearch = useCallback(
    async (q: string) => {
      if (!q.trim() || activeGuildIds.length === 0) {
        onResults([]);
        return;
      }
      setIsSearching(true);
      try {
        const res = await searchChunks(q, activeGuildIds);
        onResults(res);
      } catch {
        onResults([]);
      } finally {
        setIsSearching(false);
      }
    },
    [activeGuildIds, onResults],
  );

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      doSearch(query);
    }
  };

  const handleClear = () => {
    setQuery("");
    onResults([]);
    inputRef.current?.focus();
  };

  return (
    <div className="flex flex-col h-full">
      {/* Search input */}
      <div className="p-3 border-b border-white/10">
        <div className="relative">
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Search... (Enter)"
            className="w-full bg-white/5 border border-white/10 rounded-lg
                       py-2 pl-9 pr-8 text-sm text-white placeholder-white/30
                       focus:outline-none focus:border-galaxy-accent/50
                       focus:ring-1 focus:ring-galaxy-accent/30 transition-all"
          />
          <svg
            className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-white/30"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
            />
          </svg>
          {isSearching ? (
            <div className="absolute right-3 top-1/2 -translate-y-1/2">
              <div className="w-4 h-4 border-2 border-white/20 border-t-galaxy-accent rounded-full animate-spin" />
            </div>
          ) : (
            query && (
              <button
                onClick={handleClear}
                className="absolute right-2 top-1/2 -translate-y-1/2
                           w-5 h-5 flex items-center justify-center
                           text-white/30 hover:text-white/60 transition-colors"
              >
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            )
          )}
        </div>
      </div>

      {/* Results list */}
      <div className="flex-1 overflow-y-auto">
        {results.length === 0 && query.trim() && !isSearching && (
          <div className="p-4 text-center text-white/20 text-sm">
            No results found
          </div>
        )}
        {results.length === 0 && !query.trim() && (
          <div className="p-4 text-center text-white/20 text-sm">
            Search across all loaded chunks
          </div>
        )}
        {results.map((r, i) => {
          const isSelected = r.chunk_id === selectedChunkId;
          return (
            <button
              key={r.chunk_id}
              onClick={() => onSelectResult(r.chunk_id)}
              className={`w-full text-left px-3 py-2.5 border-b border-white/5
                         transition-colors
                         ${isSelected
                           ? "bg-galaxy-accent/15"
                           : "hover:bg-white/5"}`}
            >
              <div className="flex items-center gap-2 mb-1">
                <span className="text-white/40 text-[10px] font-mono">
                  #{i + 1}
                </span>
                {r.channel_name && (
                  <span className="text-[10px] text-white/30 truncate">
                    # {r.channel_name}
                  </span>
                )}
                <span className="ml-auto text-[10px] font-mono px-1.5 py-0.5
                                rounded bg-galaxy-accent/20 text-galaxy-accent shrink-0">
                  {r.distance.toFixed(4)}
                </span>
              </div>
              <p className="text-xs text-white/50 line-clamp-2 leading-relaxed">
                {r.preview || "..."}
              </p>
            </button>
          );
        })}
      </div>
    </div>
  );
}
