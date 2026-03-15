uniform float uPointSize;
uniform float uPixelRatio;

attribute vec3 aPickColor;

varying vec3 vPickColor;

void main() {
  vPickColor = aPickColor;

  vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
  gl_Position = projectionMatrix * mvPosition;

  float dist = -mvPosition.z;
  gl_PointSize = uPointSize * uPixelRatio * (300.0 / max(dist, 1.0));
  gl_PointSize = max(gl_PointSize, 1.0);
}
