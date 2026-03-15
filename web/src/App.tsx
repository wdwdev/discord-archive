import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import * as THREE from "three";
import GalaxyCanvas from "./components/GalaxyCanvas";
import LeftSidebar from "./components/LeftSidebar";
import SearchPanel from "./components/SearchPanel";
import ChunkPanel from "./components/ChunkPanel";
import Tooltip from "./components/Tooltip";
import { fetchGuilds, fetchChannels, fetchProjection, fetchChunk } from "./api";
import type {
  Guild,
  Channel,
  ProjectionData,
  SearchResult,
  ColorMode,
} from "./types";

/** Deterministic hue from an ID string (0–1). */
function idToHue(id: string): number {
  let h = 0;
  for (let i = 0; i < id.length; i++) {
    h = (h * 31 + id.charCodeAt(i)) | 0;
  }
  return ((h % 360) + 360) % 360 / 360;
}

export default function App() {
  // Data state
  const [guilds, setGuilds] = useState<Guild[]>([]);
  const [channels, setChannels] = useState<Map<string, Channel[]>>(new Map());
  const [projections, setProjections] = useState<Map<string, ProjectionData>>(
    new Map(),
  );

  // UI state
  const [selectedGuildIds, setSelectedGuildIds] = useState<Set<string>>(
    new Set(),
  );
  const [loadingGuildIds, setLoadingGuildIds] = useState<Set<string>>(
    new Set(),
  );
  const [hiddenChannelIds, setHiddenChannelIds] = useState<Set<string>>(
    new Set(),
  );
  const [colorMode, setColorMode] = useState<ColorMode>("guild");
  const [showStars, setShowStars] = useState(true);
  const [brightness, setBrightness] = useState(0.08);
  const [rightSidebarOpen, setRightSidebarOpen] = useState(true);

  // Interaction state
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [selectedChunkId, setSelectedChunkId] = useState<number | null>(null);
  const [hoverChunkId, setHoverChunkId] = useState<number | null>(null);
  const [hoverPos, setHoverPos] = useState<{ x: number; y: number } | null>(
    null,
  );
  const [hoverText, setHoverText] = useState<string | null>(null);
  const hoverCacheRef = useRef<Map<number, string>>(new Map());

  // Load guild list on mount
  useEffect(() => {
    fetchGuilds().then(setGuilds);
  }, []);

  // Active projections (selected & loaded)
  const activeProjections = useMemo(() => {
    const result: ProjectionData[] = [];
    for (const gid of selectedGuildIds) {
      const p = projections.get(gid);
      if (p) result.push(p);
    }
    return result;
  }, [selectedGuildIds, projections]);

  // Compute selected index (global index across active projections)
  const selectedIndex = useMemo(() => {
    if (selectedChunkId == null) return null;
    let offset = 0;
    for (const proj of activeProjections) {
      for (let i = 0; i < proj.numPoints; i++) {
        if (Number(proj.chunkIds[i]) === selectedChunkId) {
          return offset + i;
        }
      }
      offset += proj.numPoints;
    }
    return null;
  }, [selectedChunkId, activeProjections]);

  // ---------------------------------------------------------------------------
  // Color maps (stable: based on ID hash, not order)
  // ---------------------------------------------------------------------------
  const guildColors = useMemo(() => {
    const map = new Map<string, THREE.Color>();
    for (const g of guilds) {
      const hue = idToHue(g.guild_id);
      map.set(g.guild_id, new THREE.Color().setHSL(hue, 0.7, 0.55));
    }
    return map;
  }, [guilds]);

  const channelColors = useMemo(() => {
    const map = new Map<string, THREE.Color>();
    for (const [, chList] of channels) {
      for (const ch of chList) {
        const hue = idToHue(ch.channel_id);
        map.set(ch.channel_id, new THREE.Color().setHSL(hue, 0.65, 0.5));
      }
    }
    return map;
  }, [channels]);

  // ---------------------------------------------------------------------------
  // Guild toggle: load/unload projection
  // ---------------------------------------------------------------------------
  const toggleGuild = useCallback(
    async (guildId: string) => {
      const newSelected = new Set(selectedGuildIds);

      if (newSelected.has(guildId)) {
        newSelected.delete(guildId);
        setSelectedGuildIds(newSelected);
        return;
      }

      newSelected.add(guildId);
      setSelectedGuildIds(newSelected);

      // Load projection if not cached
      if (!projections.has(guildId)) {
        setLoadingGuildIds((prev) => new Set(prev).add(guildId));

        try {
          const [proj, chans] = await Promise.all([
            fetchProjection(guildId),
            fetchChannels(guildId),
          ]);
          setProjections((prev) => new Map(prev).set(guildId, proj));
          setChannels((prev) => new Map(prev).set(guildId, chans));
        } catch (e) {
          console.error("Failed to load projection:", e);
          newSelected.delete(guildId);
          setSelectedGuildIds(new Set(newSelected));
        } finally {
          setLoadingGuildIds((prev) => {
            const next = new Set(prev);
            next.delete(guildId);
            return next;
          });
        }
      } else if (!channels.has(guildId)) {
        fetchChannels(guildId).then((chans) => {
          setChannels((prev) => new Map(prev).set(guildId, chans));
        });
      }
    },
    [selectedGuildIds, projections, channels],
  );

  // ---------------------------------------------------------------------------
  // Hover: fetch chunk text for tooltip
  // ---------------------------------------------------------------------------
  const handleHover = useCallback(
    (chunkId: number | null, screenPos: { x: number; y: number } | null) => {
      setHoverChunkId(chunkId);
      setHoverPos(screenPos);

      if (chunkId == null) {
        setHoverText(null);
        return;
      }

      const cached = hoverCacheRef.current.get(chunkId);
      if (cached) {
        setHoverText(cached);
        return;
      }

      setHoverText(null);
      fetchChunk(chunkId).then((detail) => {
        const preview = detail.text.slice(0, 200);
        hoverCacheRef.current.set(chunkId, preview);
        setHoverText(preview);
      });
    },
    [],
  );

  const handleClick = useCallback((chunkId: number | null) => {
    setSelectedChunkId(chunkId);
  }, []);

  // ---------------------------------------------------------------------------
  // Search
  // ---------------------------------------------------------------------------
  const handleSearchResults = useCallback((results: SearchResult[]) => {
    setSearchResults(results);
    if (results.length > 0) {
      setSelectedChunkId(results[0].chunk_id);
    }
  }, []);

  const handleSelectResult = useCallback((chunkId: number) => {
    setSelectedChunkId(chunkId);
  }, []);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------
  return (
    <div className="h-screen w-screen relative overflow-hidden">
      {/* 3D Canvas (full screen) */}
      <div className="absolute inset-0 z-0">
        <GalaxyCanvas
          projections={activeProjections}
          colorMode={colorMode}
          showStars={showStars}
          brightness={brightness}
          guildColors={guildColors}
          channelColors={channelColors}
          hiddenChannelIds={hiddenChannelIds}
          searchResults={searchResults}
          selectedIndex={selectedIndex}
          onHover={handleHover}
          onClick={handleClick}
        />
      </div>

      {/* Hover tooltip */}
      {hoverChunkId != null && hoverPos && (
        <Tooltip
          chunkId={hoverChunkId}
          position={hoverPos}
          text={hoverText}
        />
      )}

      {/* Left sidebar: guild/channel controls */}
      <LeftSidebar
        guilds={guilds}
        channels={channels}
        selectedGuildIds={selectedGuildIds}
        hiddenChannelIds={hiddenChannelIds}
        colorMode={colorMode}
        showStars={showStars}
        loadingGuildIds={loadingGuildIds}
        guildColors={guildColors}
        channelColors={channelColors}
        onToggleGuild={toggleGuild}
        onToggleChannel={(id: string) => {
          const next = new Set(hiddenChannelIds);
          if (next.has(id)) next.delete(id);
          else next.add(id);
          setHiddenChannelIds(next);
        }}
        onToggleAllChannels={(guildId: string, show: boolean) => {
          const chList = channels.get(guildId);
          if (!chList) return;
          const next = new Set(hiddenChannelIds);
          for (const ch of chList) {
            if (show) next.delete(ch.channel_id);
            else next.add(ch.channel_id);
          }
          setHiddenChannelIds(next);
        }}
        onSetColorMode={setColorMode}
        onToggleStars={() => setShowStars((v) => !v)}
        brightness={brightness}
        onBrightnessChange={setBrightness}
      />

      {/* Right sidebar: search + chunk detail */}
      <div
        className={`absolute right-0 top-0 h-full z-20 w-[340px]
                    bg-black/40 backdrop-blur-xl border-l border-white/10
                    flex flex-col transition-transform duration-300 ease-out
                    ${rightSidebarOpen ? "translate-x-0" : "translate-x-full"}`}
      >
        {/* Search panel (top half) */}
        <div className="flex-1 min-h-0 border-b border-white/10 flex flex-col">
          <SearchPanel
            activeGuildIds={Array.from(selectedGuildIds)}
            onResults={handleSearchResults}
            onSelectResult={handleSelectResult}
            results={searchResults}
            selectedChunkId={selectedChunkId}
          />
        </div>

        {/* Chunk detail (bottom half) */}
        <div className="flex-1 min-h-0 flex flex-col">
          <ChunkPanel chunkId={selectedChunkId} />
        </div>
      </div>

      {/* Right sidebar toggle */}
      <button
        onClick={() => setRightSidebarOpen(!rightSidebarOpen)}
        className="absolute right-0 top-6 z-30 bg-white/5 backdrop-blur-md
                   border border-white/10 rounded-l-lg px-2 py-3 text-white/60
                   hover:text-white hover:bg-white/10 transition-all"
        style={{
          transform: rightSidebarOpen ? "translateX(-340px)" : "none",
        }}
      >
        <svg
          className={`w-4 h-4 transition-transform ${rightSidebarOpen ? "" : "rotate-180"}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2}
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M15 19l-7-7 7-7"
          />
        </svg>
      </button>

      {/* Empty state */}
      {activeProjections.length === 0 && (
        <div className="absolute inset-0 z-10 flex items-center justify-center pointer-events-none">
          <div className="text-center">
            <h2 className="text-2xl font-semibold text-white/60 mb-2">
              Select a guild to begin
            </h2>
            <p className="text-white/30 text-sm">
              Choose one or more guilds from the left panel
            </p>
          </div>
        </div>
      )}
    </div>
  );
}
