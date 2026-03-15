import { useRef, useMemo, useEffect, useCallback } from "react";
import { useThree, useFrame } from "@react-three/fiber";
import * as THREE from "three";
import type { ProjectionData, ColorMode, SearchResult } from "../types";

import pointVertSrc from "../shaders/point.vert.glsl?raw";
import pointFragSrc from "../shaders/point.frag.glsl?raw";
import pickVertSrc from "../shaders/pick.vert.glsl?raw";
import pickFragSrc from "../shaders/pick.frag.glsl?raw";

const DIM_OPACITY = 0.3;

interface Props {
  projections: ProjectionData[];
  colorMode: ColorMode;
  brightness: number;
  guildColors: Map<string, THREE.Color>;
  channelColors: Map<string, THREE.Color>;
  hiddenChannelIds: Set<string>;
  searchResults: SearchResult[];
  selectedIndex: number | null;
  onHover: (
    chunkId: number | null,
    screenPos: { x: number; y: number } | null,
  ) => void;
  onClick: (chunkId: number | null) => void;
}

export default function PointCloud({
  projections,
  colorMode,
  brightness,
  guildColors,
  channelColors,
  hiddenChannelIds,
  searchResults,
  selectedIndex,
  onHover,
  onClick,
}: Props) {
  const { gl, camera, size } = useThree();
  const pointsRef = useRef<THREE.Points>(null);

  // Build a lookup: chunk_id → global index for fast search result matching
  const { totalPoints, mergedPositions, chunkIdToIndex, chunkIds, channelIdArr, guildIdArr } =
    useMemo(() => {
      let total = 0;
      for (const p of projections) total += p.numPoints;

      const pos = new Float32Array(total * 3);
      const cids: bigint[] = new Array(total);
      const chanIds: string[] = new Array(total);
      const gids: string[] = new Array(total);
      const lookup = new Map<number, number>();

      let offset = 0;
      for (const proj of projections) {
        pos.set(proj.positions, offset * 3);
        for (let i = 0; i < proj.numPoints; i++) {
          const idx = offset + i;
          cids[idx] = proj.chunkIds[i];
          chanIds[idx] = String(proj.channelIds[i]);
          gids[idx] = proj.guildId;
          lookup.set(Number(proj.chunkIds[i]), idx);
        }
        offset += proj.numPoints;
      }

      return {
        totalPoints: total,
        mergedPositions: pos,
        chunkIdToIndex: lookup,
        chunkIds: cids,
        channelIdArr: chanIds,
        guildIdArr: gids,
      };
    }, [projections]);

  // -----------------------------------------------------------------------
  // Color + opacity buffers
  // -----------------------------------------------------------------------
  const colorBuf = useMemo(
    () => new Float32Array(totalPoints * 3),
    [totalPoints],
  );
  const opacityBuf = useMemo(
    () => new Float32Array(totalPoints).fill(1),
    [totalPoints],
  );
  // Update colors when colorMode or projections change
  useEffect(() => {
    const defaultColor = new THREE.Color(0.6, 0.6, 0.6);
    for (let i = 0; i < totalPoints; i++) {
      const id =
        colorMode === "guild" ? guildIdArr[i] : channelIdArr[i];
      const colorMap = colorMode === "guild" ? guildColors : channelColors;
      const c = colorMap.get(id) ?? defaultColor;
      colorBuf[i * 3] = c.r;
      colorBuf[i * 3 + 1] = c.g;
      colorBuf[i * 3 + 2] = c.b;
    }

    if (pointsRef.current) {
      const geom = pointsRef.current.geometry;
      geom.getAttribute("aColor").needsUpdate = true;
    }
  }, [colorMode, guildColors, channelColors, totalPoints, guildIdArr, channelIdArr, colorBuf]);

  // Update opacity for hidden channels + search results (highlight + dim)
  useEffect(() => {
    // Base opacity: 1 for visible, 0 for hidden channels
    for (let i = 0; i < totalPoints; i++) {
      opacityBuf[i] = hiddenChannelIds.has(channelIdArr[i]) ? 0 : 1;
    }

    // Search: dim non-hidden to DIM_OPACITY, highlight matches
    if (searchResults.length > 0) {
      for (let i = 0; i < totalPoints; i++) {
        if (opacityBuf[i] > 0) opacityBuf[i] = DIM_OPACITY;
      }
      const highlightColor = new THREE.Color();
      for (const r of searchResults) {
        const idx = chunkIdToIndex.get(r.chunk_id);
        if (idx == null || opacityBuf[idx] === 0) continue;
        opacityBuf[idx] = 1.0;

        // Color by similarity: green (close) → yellow → orange (far)
        const maxDist = searchResults[searchResults.length - 1]?.distance ?? 1;
        const minDist = searchResults[0]?.distance ?? 0;
        const range = maxDist - minDist || 1;
        const t = (r.distance - minDist) / range;
        highlightColor.setHSL(0.33 - t * 0.25, 0.9, 0.55);
        colorBuf[idx * 3] = highlightColor.r;
        colorBuf[idx * 3 + 1] = highlightColor.g;
        colorBuf[idx * 3 + 2] = highlightColor.b;
      }
    }

    if (pointsRef.current) {
      const geom = pointsRef.current.geometry;
      geom.getAttribute("aOpacity").needsUpdate = true;
      geom.getAttribute("aColor").needsUpdate = true;
    }
  }, [searchResults, hiddenChannelIds, chunkIdToIndex, totalPoints, channelIdArr, opacityBuf, colorBuf]);

  // Selected point opacity is now handled in the shader via uSelectedIndex

  // -----------------------------------------------------------------------
  // Main shader material
  // -----------------------------------------------------------------------
  const material = useMemo(
    () =>
      new THREE.ShaderMaterial({
        vertexShader: pointVertSrc,
        fragmentShader: pointFragSrc,
        uniforms: {
          uPointSize: { value: 0.1 },
          uPixelRatio: { value: Math.min(window.devicePixelRatio, 2) },
          uTime: { value: 0 },
          uSelectedIndex: { value: -1 },
          uBrightness: { value: 0.08 },
        },
        transparent: true,
        depthWrite: false,
        blending: THREE.AdditiveBlending,
      }),
    [],
  );

  // Update uniforms
  useEffect(() => {
    material.uniforms.uSelectedIndex.value = selectedIndex ?? -1;
  }, [selectedIndex, material]);

  useEffect(() => {
    material.uniforms.uBrightness.value = brightness;
  }, [brightness, material]);

  // Animate time uniform for breathing effect
  useFrame((state) => {
    material.uniforms.uTime.value = state.clock.elapsedTime;
  });

  // -----------------------------------------------------------------------
  // GPU Picking
  // -----------------------------------------------------------------------
  const pickScene = useMemo(() => new THREE.Scene(), []);
  const pickTarget = useMemo(
    () => new THREE.WebGLRenderTarget(1, 1, { format: THREE.RGBAFormat }),
    [],
  );
  const pickPixel = useMemo(() => new Uint8Array(4), []);

  // Pick color buffer: encode index as RGB
  const pickColorBuf = useMemo(() => {
    const buf = new Float32Array(totalPoints * 3);
    for (let i = 0; i < totalPoints; i++) {
      // Encode index as R,G,B in [0,1] — supports up to 16M points per channel
      buf[i * 3] = ((i + 1) & 0xff) / 255;
      buf[i * 3 + 1] = (((i + 1) >> 8) & 0xff) / 255;
      buf[i * 3 + 2] = (((i + 1) >> 16) & 0xff) / 255;
    }
    return buf;
  }, [totalPoints]);

  const pickMaterial = useMemo(
    () =>
      new THREE.ShaderMaterial({
        vertexShader: pickVertSrc,
        fragmentShader: pickFragSrc,
        uniforms: {
          uPointSize: { value: 0.1 },
          uPixelRatio: { value: Math.min(window.devicePixelRatio, 2) },
        },
      }),
    [],
  );

  // Build pick scene geometry (mirrors main geometry, shares opacity buffer)
  const pickPoints = useMemo(() => {
    if (totalPoints === 0) return null;
    const geom = new THREE.BufferGeometry();
    geom.setAttribute(
      "position",
      new THREE.BufferAttribute(mergedPositions, 3),
    );
    geom.setAttribute(
      "aPickColor",
      new THREE.BufferAttribute(pickColorBuf, 3),
    );
    return new THREE.Points(geom, pickMaterial);
  }, [mergedPositions, pickColorBuf, pickMaterial, totalPoints]);

  useEffect(() => {
    pickScene.clear();
    if (pickPoints) pickScene.add(pickPoints);
  }, [pickScene, pickPoints]);

  const doPick = useCallback(
    (screenX: number, screenY: number): number | null => {
      if (totalPoints === 0) return null;

      const rect = gl.domElement.getBoundingClientRect();
      const x = ((screenX - rect.left) / rect.width) * size.width;
      const y = ((screenY - rect.top) / rect.height) * size.height;

      // Render pick scene at 1x1 pixel at mouse position
      const cam = camera as THREE.PerspectiveCamera;
      gl.setRenderTarget(pickTarget);
      gl.setScissorTest(true);
      gl.setScissor(x, size.height - y, 1, 1);
      gl.setViewport(0, 0, size.width, size.height);
      gl.clear();
      gl.render(pickScene, cam);
      gl.readRenderTargetPixels(pickTarget, x, size.height - y, 1, 1, pickPixel);
      gl.setScissorTest(false);
      gl.setRenderTarget(null);

      const [r, g, b] = pickPixel;
      const id = r + (g << 8) + (b << 16);
      if (id === 0) return null; // Background
      return id - 1; // Convert back to 0-based index
    },
    [gl, camera, size, pickScene, pickTarget, pickPixel, totalPoints],
  );

  // -----------------------------------------------------------------------
  // Mouse events
  // -----------------------------------------------------------------------
  useEffect(() => {
    const canvas = gl.domElement;
    let downX = 0;
    let downY = 0;
    const DRAG_THRESHOLD = 4; // px — movement beyond this = drag, not click

    const handleMove = (e: MouseEvent) => {
      const idx = doPick(e.clientX, e.clientY);
      if (idx != null && idx < totalPoints) {
        const cid = Number(chunkIds[idx]);
        onHover(cid, { x: e.clientX, y: e.clientY });
        canvas.style.cursor = "pointer";
      } else {
        onHover(null, null);
        canvas.style.cursor = "default";
      }
    };

    const handleDown = (e: MouseEvent) => {
      downX = e.clientX;
      downY = e.clientY;
    };

    const handleUp = (e: MouseEvent) => {
      const dx = e.clientX - downX;
      const dy = e.clientY - downY;
      if (dx * dx + dy * dy > DRAG_THRESHOLD * DRAG_THRESHOLD) return; // was a drag

      const idx = doPick(e.clientX, e.clientY);
      if (idx != null && idx < totalPoints) {
        onClick(Number(chunkIds[idx]));
      } else {
        onClick(null);
      }
    };

    canvas.addEventListener("mousemove", handleMove);
    canvas.addEventListener("mousedown", handleDown);
    canvas.addEventListener("mouseup", handleUp);
    return () => {
      canvas.removeEventListener("mousemove", handleMove);
      canvas.removeEventListener("mousedown", handleDown);
      canvas.removeEventListener("mouseup", handleUp);
    };
  }, [gl, doPick, totalPoints, chunkIds, onHover, onClick]);

  // Update pick render target size
  useEffect(() => {
    pickTarget.setSize(size.width, size.height);
  }, [size, pickTarget]);

  // -----------------------------------------------------------------------
  // Render
  // -----------------------------------------------------------------------
  if (totalPoints === 0) return null;

  return (
    <points ref={pointsRef} material={material} frustumCulled={false}>
      <bufferGeometry>
        <bufferAttribute
          attach="attributes-position"
          args={[mergedPositions, 3]}
        />
        <bufferAttribute
          attach="attributes-aColor"
          args={[colorBuf, 3]}
        />
        <bufferAttribute
          attach="attributes-aOpacity"
          args={[opacityBuf, 1]}
        />
      </bufferGeometry>
    </points>
  );
}
