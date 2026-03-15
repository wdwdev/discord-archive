import { Canvas, useThree } from "@react-three/fiber";
import { OrbitControls, Stars } from "@react-three/drei";
import { EffectComposer, Bloom } from "@react-three/postprocessing";
import { Suspense, useRef, useEffect } from "react";
import * as THREE from "three";
import type { OrbitControls as OrbitControlsImpl } from "three-stdlib";
import PointCloud from "./PointCloud";
import type { ProjectionData, ColorMode, SearchResult } from "../types";

interface Props {
  projections: ProjectionData[];
  colorMode: ColorMode;
  showStars: boolean;
  brightness: number;
  guildColors: Map<string, THREE.Color>;
  channelColors: Map<string, THREE.Color>;
  hiddenChannelIds: Set<string>;
  searchResults: SearchResult[];
  selectedIndex: number | null;
  onHover: (chunkId: number | null, screenPos: { x: number; y: number } | null) => void;
  onClick: (chunkId: number | null) => void;
}

/** Inner component that can use useThree() for camera control. */
function CameraController({
  projections,
  selectedIndex,
  controlsRef,
}: {
  projections: ProjectionData[];
  selectedIndex: number | null;
  controlsRef: React.RefObject<OrbitControlsImpl | null>;
}) {
  const { camera, invalidate } = useThree();
  const hasInitialized = useRef(false);
  const flyDistRef = useRef<number | null>(null);
  const flyingRef = useRef(false);

  // Fit camera to all points when projections change
  useEffect(() => {
    if (projections.length === 0 || !controlsRef.current) return;

    // Compute bounding box efficiently (sample every Nth point for speed)
    const box = new THREE.Box3();
    const v = new THREE.Vector3();
    for (const proj of projections) {
      const step = Math.max(1, Math.floor(proj.numPoints / 10000));
      for (let i = 0; i < proj.numPoints; i += step) {
        v.set(
          proj.positions[i * 3],
          proj.positions[i * 3 + 1],
          proj.positions[i * 3 + 2],
        );
        box.expandByPoint(v);
      }
    }

    const center = box.getCenter(new THREE.Vector3());
    const size = box.getSize(new THREE.Vector3());
    const maxDim = Math.max(size.x, size.y, size.z);
    const fov = ((camera as THREE.PerspectiveCamera).fov * Math.PI) / 180;
    const dist = Math.abs(maxDim / Math.tan(fov / 2)) * 0.8;

    if (!hasInitialized.current) {
      // Instant move on first load
      camera.position.set(center.x, center.y + dist * 0.3, center.z + dist);
      controlsRef.current.target.copy(center);
      controlsRef.current.update();
      hasInitialized.current = true;
    } else {
      // Animate to new position on subsequent loads
      const startPos = camera.position.clone();
      const startTarget = controlsRef.current.target.clone();
      const endPos = new THREE.Vector3(
        center.x,
        center.y + dist * 0.3,
        center.z + dist,
      );

      let t = 0;
      const animate = () => {
        t += 0.03;
        const ease = 1 - Math.pow(1 - Math.min(t, 1), 3);
        camera.position.lerpVectors(startPos, endPos, ease);
        controlsRef.current!.target.lerpVectors(startTarget, center, ease);
        controlsRef.current!.update();
        invalidate();
        if (t < 1) requestAnimationFrame(animate);
      };
      requestAnimationFrame(animate);
    }
    invalidate();
  }, [projections, camera, controlsRef, invalidate]);

  // Fly camera to selected point
  useEffect(() => {
    if (selectedIndex == null || !controlsRef.current) return;

    let targetPos: THREE.Vector3 | null = null;
    let remaining = selectedIndex;
    for (const proj of projections) {
      if (remaining < proj.numPoints) {
        targetPos = new THREE.Vector3(
          proj.positions[remaining * 3],
          proj.positions[remaining * 3 + 1],
          proj.positions[remaining * 3 + 2],
        );
        break;
      }
      remaining -= proj.numPoints;
    }
    if (!targetPos) return;

    const controls = controlsRef.current;
    const startPos = camera.position.clone();
    const startTarget = controls.target.clone();

    // Capture zoom distance before first fly-to; reuse for subsequent ones
    if (!flyingRef.current) {
      flyDistRef.current = startPos.distanceTo(startTarget);
    }
    const zoomDist = flyDistRef.current!;

    const dir = new THREE.Vector3()
      .subVectors(startPos, startTarget)
      .normalize();
    const endPos = targetPos.clone().add(dir.multiplyScalar(zoomDist));

    controls.enabled = false;
    flyingRef.current = true;
    let t = 0;
    const animate = () => {
      t += 0.02;
      const ease = 1 - Math.pow(1 - Math.min(t, 1), 3);
      camera.position.lerpVectors(startPos, endPos, ease);
      controls.target.lerpVectors(startTarget, targetPos!, ease);
      controls.update();
      invalidate();
      if (t < 1) {
        requestAnimationFrame(animate);
      } else {
        controls.enabled = true;
        flyingRef.current = false;
      }
    };
    requestAnimationFrame(animate);
  }, [selectedIndex, projections, camera, controlsRef, invalidate]);

  return null;
}

export default function GalaxyCanvas({
  projections,
  colorMode,
  showStars,
  brightness,
  guildColors,
  channelColors,
  hiddenChannelIds,
  searchResults,
  selectedIndex,
  onHover,
  onClick,
}: Props) {
  const controlsRef = useRef<OrbitControlsImpl>(null);

  return (
    <Canvas
      camera={{ position: [0, 30, 120], fov: 50 }}
      gl={{ antialias: false, alpha: false, toneMapping: THREE.ACESFilmicToneMapping, toneMappingExposure: 1.5 }}
    >
      <color attach="background" args={["#08080b"]} />
      <ambientLight intensity={0.3} />

      <Suspense fallback={null}>
        <PointCloud
          projections={projections}
          colorMode={colorMode}
          brightness={brightness}
          guildColors={guildColors}
          channelColors={channelColors}
          hiddenChannelIds={hiddenChannelIds}
          searchResults={searchResults}
          selectedIndex={selectedIndex}
          onHover={onHover}
          onClick={onClick}
        />

        {showStars && (
          <Stars
            radius={300}
            depth={100}
            count={3000}
            factor={6}
            saturation={0.8}
            fade
            speed={0.5}
          />
        )}

        <OrbitControls
          ref={controlsRef}
          makeDefault
          enableDamping
          dampingFactor={0.08}
          rotateSpeed={0.5}
          zoomSpeed={1.2}
        />

        <CameraController
          projections={projections}
          selectedIndex={selectedIndex}
          controlsRef={controlsRef}
        />

        <EffectComposer enabled>
          <Bloom
            luminanceThreshold={0.15}
            luminanceSmoothing={0.7}
            intensity={0.8}
            mipmapBlur
          />
        </EffectComposer>
      </Suspense>
    </Canvas>
  );
}
