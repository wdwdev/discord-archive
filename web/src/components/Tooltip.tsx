interface Props {
  chunkId: number;
  position: { x: number; y: number };
  text: string | null;
}

export default function Tooltip({ chunkId, position, text }: Props) {
  return (
    <div
      className="fixed z-50 pointer-events-none bg-black/80 backdrop-blur-md
                 border border-white/15 rounded-lg px-3 py-2 text-xs
                 shadow-xl max-w-xs"
      style={{
        left: position.x + 16,
        top: position.y - 8,
        transform: "translateY(-100%)",
      }}
    >
      <div className="text-white/40 text-[10px] mb-0.5">
        chunk {chunkId}
      </div>
      {text ? (
        <div className="text-white/80 line-clamp-3 whitespace-pre-wrap">
          {text}
        </div>
      ) : (
        <div className="text-white/40 italic">Loading...</div>
      )}
    </div>
  );
}
