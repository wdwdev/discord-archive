import { useState } from "react";
import type { Color } from "three";
import type { Guild, Channel, ColorMode } from "../types";

interface Props {
  guilds: Guild[];
  channels: Map<string, Channel[]>;
  selectedGuildIds: Set<string>;
  hiddenChannelIds: Set<string>;
  colorMode: ColorMode;
  showStars: boolean;
  brightness: number;
  loadingGuildIds: Set<string>;
  guildColors: Map<string, Color>;
  channelColors: Map<string, Color>;
  onToggleGuild: (guildId: string) => void;
  onToggleChannel: (channelId: string) => void;
  onToggleAllChannels: (guildId: string, show: boolean) => void;
  onSetColorMode: (mode: ColorMode) => void;
  onToggleStars: () => void;
  onBrightnessChange: (value: number) => void;
}

function colorToCSS(c: Color | undefined): string {
  if (!c) return "rgb(128,128,128)";
  return `rgb(${Math.round(c.r * 255)},${Math.round(c.g * 255)},${Math.round(c.b * 255)})`;
}

export default function LeftSidebar({
  guilds,
  channels,
  selectedGuildIds,
  hiddenChannelIds,
  colorMode,
  showStars,
  brightness,
  loadingGuildIds,
  guildColors,
  channelColors,
  onToggleGuild,
  onToggleChannel,
  onToggleAllChannels,
  onSetColorMode,
  onToggleStars,
  onBrightnessChange,
}: Props) {
  const [collapsed, setCollapsed] = useState(false);
  const [collapsedGuilds, setCollapsedGuilds] = useState<Set<string>>(
    new Set(),
  );

  return (
    <>
      {/* Toggle button */}
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="absolute left-0 top-6 z-30 bg-white/5 backdrop-blur-md
                   border border-white/10 rounded-r-lg px-2 py-3 text-white/60
                   hover:text-white hover:bg-white/10 transition-all"
        style={{ transform: collapsed ? "none" : "translateX(280px)" }}
      >
        <svg
          className={`w-4 h-4 transition-transform ${collapsed ? "" : "rotate-180"}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2}
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M9 5l7 7-7 7"
          />
        </svg>
      </button>

      {/* Sidebar panel */}
      <div
        className={`absolute left-0 top-0 h-full z-20 w-[280px] bg-black/40
                    backdrop-blur-xl border-r border-white/10 flex flex-col
                    transition-transform duration-300 ease-out
                    ${collapsed ? "-translate-x-full" : "translate-x-0"}`}
      >
        <div className="p-4 border-b border-white/10">
          <h1 className="text-lg font-semibold tracking-tight">
            Semantic Galaxy
          </h1>
          <p className="text-xs text-white/40 mt-0.5">
            Discord Archive Visualizer
          </p>
        </div>

        {/* Guild list */}
        <div className="flex-1 overflow-y-auto p-3 space-y-1">
          <div className="text-[11px] font-medium text-white/30 uppercase tracking-wider px-2 mb-2">
            Guilds
          </div>
          {guilds.map((g) => {
            const isSelected = selectedGuildIds.has(g.guild_id);
            const isLoading = loadingGuildIds.has(g.guild_id);
            const guildChannels = channels.get(g.guild_id) ?? [];
            const textChannels = guildChannels.filter((ch) => ch.type === 0);
            const isCollapsed = collapsedGuilds.has(g.guild_id);
            const showChannels = isSelected && textChannels.length > 0 && !isCollapsed;

            // Count how many channels of this guild are visible
            const visibleCount = textChannels.filter(
              (ch) => !hiddenChannelIds.has(ch.channel_id),
            ).length;
            const allVisible = visibleCount === textChannels.length;
            const noneVisible = visibleCount === 0;

            return (
              <div key={g.guild_id}>
                {/* Guild row */}
                <div className="flex items-center gap-1">
                  {/* Checkbox */}
                  <button
                    onClick={() => onToggleGuild(g.guild_id)}
                    className="shrink-0 w-5 h-5 flex items-center justify-center"
                  >
                    {isLoading ? (
                      <span className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    ) : (
                      <span
                        className={`w-3.5 h-3.5 rounded border transition-colors flex items-center justify-center
                                   ${isSelected
                                     ? "border-transparent"
                                     : "border-white/20 hover:border-white/40"}`}
                        style={{
                          backgroundColor: isSelected
                            ? colorToCSS(guildColors.get(g.guild_id))
                            : "transparent",
                        }}
                      >
                        {isSelected && (
                          <svg className="w-2.5 h-2.5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                            <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                          </svg>
                        )}
                      </span>
                    )}
                  </button>

                  {/* Guild name (click to collapse/expand channels) */}
                  <button
                    onClick={() => {
                      if (!isSelected) {
                        onToggleGuild(g.guild_id);
                        return;
                      }
                      const next = new Set(collapsedGuilds);
                      if (isCollapsed) next.delete(g.guild_id);
                      else next.add(g.guild_id);
                      setCollapsedGuilds(next);
                    }}
                    className={`flex-1 text-left py-1.5 text-sm truncate transition-colors
                               ${isSelected
                                 ? "text-white"
                                 : "text-white/50 hover:text-white/80"}`}
                  >
                    {g.name}
                  </button>

                  {/* Collapse arrow */}
                  {isSelected && textChannels.length > 0 && (
                    <button
                      onClick={() => {
                        const next = new Set(collapsedGuilds);
                        if (isCollapsed) next.delete(g.guild_id);
                        else next.add(g.guild_id);
                        setCollapsedGuilds(next);
                      }}
                      className="text-white/30 hover:text-white/60 px-1 shrink-0"
                    >
                      <svg
                        className={`w-3 h-3 transition-transform ${isCollapsed ? "" : "rotate-90"}`}
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        strokeWidth={2}
                      >
                        <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
                      </svg>
                    </button>
                  )}
                </div>

                {/* Channel list */}
                {showChannels && (
                  <div className="ml-5 mt-1 mb-1">
                    {/* Show all / Hide all */}
                    <div className="flex items-center gap-1 mb-1 px-1">
                      <button
                        onClick={() => onToggleAllChannels(g.guild_id, true)}
                        className={`text-[10px] px-1.5 py-0.5 rounded transition-colors
                                   ${allVisible
                                     ? "text-white/20 cursor-default"
                                     : "text-white/40 hover:text-white/70 hover:bg-white/5"}`}
                        disabled={allVisible}
                      >
                        Show all
                      </button>
                      <span className="text-white/10">|</span>
                      <button
                        onClick={() => onToggleAllChannels(g.guild_id, false)}
                        className={`text-[10px] px-1.5 py-0.5 rounded transition-colors
                                   ${noneVisible
                                     ? "text-white/20 cursor-default"
                                     : "text-white/40 hover:text-white/70 hover:bg-white/5"}`}
                        disabled={noneVisible}
                      >
                        Hide all
                      </button>
                      <span className="ml-auto text-[10px] text-white/20">
                        {visibleCount}/{textChannels.length}
                      </span>
                    </div>

                    {/* Channel items */}
                    <div className="space-y-0.5 max-h-48 overflow-y-auto">
                      {textChannels.map((ch) => {
                        const hidden = hiddenChannelIds.has(ch.channel_id);
                        return (
                          <button
                            key={ch.channel_id}
                            onClick={() => onToggleChannel(ch.channel_id)}
                            className={`flex items-center gap-1.5 w-full text-left px-1 py-0.5 rounded
                                       text-xs transition-colors group
                                       ${hidden
                                         ? "text-white/20"
                                         : "text-white/50 hover:text-white/80"}`}
                          >
                            {/* Color dot */}
                            <span
                              className="w-1.5 h-1.5 rounded-full shrink-0 transition-opacity"
                              style={{
                                backgroundColor: colorToCSS(channelColors.get(ch.channel_id)),
                                opacity: hidden ? 0.2 : 1,
                              }}
                            />
                            {/* Eye icon */}
                            <svg
                              className={`w-3 h-3 shrink-0 transition-opacity
                                         ${hidden ? "opacity-30" : "opacity-50 group-hover:opacity-80"}`}
                              fill="none"
                              viewBox="0 0 24 24"
                              stroke="currentColor"
                              strokeWidth={2}
                            >
                              {hidden ? (
                                <path strokeLinecap="round" strokeLinejoin="round" d="M13.875 18.825A10.05 10.05 0 0112 19c-4.478 0-8.268-2.943-9.543-7a9.97 9.97 0 011.563-3.029m5.858.908a3 3 0 114.243 4.243M9.878 9.878l4.242 4.242M3 3l18 18" />
                              ) : (
                                <>
                                  <path strokeLinecap="round" strokeLinejoin="round" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
                                  <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                                </>
                              )}
                            </svg>
                            <span className={`truncate ${hidden ? "line-through" : ""}`}>
                              # {ch.name}
                            </span>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Settings */}
        <div className="p-3 border-t border-white/10 space-y-3">
          <div className="text-[11px] font-medium text-white/30 uppercase tracking-wider px-2">
            Display
          </div>

          {/* Color mode */}
          <div className="flex gap-1 px-1">
            {(["guild", "channel"] as const).map((mode) => (
              <button
                key={mode}
                onClick={() => onSetColorMode(mode)}
                className={`flex-1 text-xs py-1.5 rounded-md transition-colors capitalize
                           ${colorMode === mode
                             ? "bg-galaxy-accent/20 text-galaxy-accent"
                             : "text-white/40 hover:text-white/60 hover:bg-white/5"}`}
              >
                {mode}
              </button>
            ))}
          </div>

          {/* Stars toggle */}
          <button
            onClick={onToggleStars}
            className="flex items-center gap-2 px-2 cursor-pointer w-full"
          >
            <div
              className={`w-8 h-4 rounded-full transition-colors relative
                         ${showStars ? "bg-galaxy-accent/40" : "bg-white/10"}`}
            >
              <div
                className={`absolute top-0.5 w-3 h-3 rounded-full bg-white transition-transform
                           ${showStars ? "translate-x-4" : "translate-x-0.5"}`}
              />
            </div>
            <span className="text-xs text-white/50">Stars</span>
          </button>

          {/* Brightness slider */}
          <div className="px-2 space-y-1">
            <div className="flex items-center justify-between">
              <span className="text-xs text-white/50">Brightness</span>
              <span className="text-[10px] text-white/30 font-mono">
                {brightness.toFixed(2)}
              </span>
            </div>
            <input
              type="range"
              min="0.005"
              max="1"
              step="0.005"
              value={brightness}
              onChange={(e) => onBrightnessChange(parseFloat(e.target.value))}
              className="w-full h-1 bg-white/10 rounded-full appearance-none cursor-pointer
                         [&::-webkit-slider-thumb]:appearance-none [&::-webkit-slider-thumb]:w-3
                         [&::-webkit-slider-thumb]:h-3 [&::-webkit-slider-thumb]:rounded-full
                         [&::-webkit-slider-thumb]:bg-white"
            />
          </div>
        </div>
      </div>
    </>
  );
}
