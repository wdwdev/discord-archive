uniform float uPointSize;
uniform float uPixelRatio;
uniform float uTime;
uniform float uSelectedIndex;

attribute vec3 aColor;
attribute float aOpacity;

varying vec3 vColor;
varying float vOpacity;
varying float vIsSelected;

void main() {
  vColor = aColor;
  vOpacity = aOpacity;

  // Check if this vertex is the selected one
  float idx = float(gl_VertexID);
  vIsSelected = step(abs(idx - uSelectedIndex), 0.5) * step(0.0, uSelectedIndex);

  vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
  gl_Position = projectionMatrix * mvPosition;

  // Size attenuation: closer points appear larger
  float dist = -mvPosition.z;
  float baseSize = uPointSize * uPixelRatio * (300.0 / max(dist, 1.0));

  // Breathing pulse for selected point
  float pulse = 1.0 + vIsSelected * 0.2 * (0.5 + 0.5 * sin(uTime * 3.0));
  gl_PointSize = max(baseSize * (1.0 + vIsSelected * 0.5) * pulse, 1.0);
}
