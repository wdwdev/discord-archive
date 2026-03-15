uniform float uBrightness;

varying vec3 vColor;
varying float vOpacity;
varying float vIsSelected;

void main() {
  vec2 center = gl_PointCoord - vec2(0.5);
  float dist = length(center);

  // Discard outside circle
  if (dist > 0.5) discard;

  // Reconstruct sphere normal from point coord
  vec3 normal = vec3(center * 2.0, sqrt(max(1.0 - 4.0 * dot(center, center), 0.0)));

  // Simple directional light from upper-right-front
  vec3 lightDir = normalize(vec3(0.4, 0.6, 0.8));
  float diffuse = max(dot(normal, lightDir), 0.0);
  float ambient = 0.15;
  float lighting = ambient + diffuse * 0.85;

  // Specular highlight
  vec3 viewDir = vec3(0.0, 0.0, 1.0);
  vec3 halfDir = normalize(lightDir + viewDir);
  float spec = pow(max(dot(normal, halfDir), 0.0), 32.0) * 0.3;

  // Rim light for depth
  float rim = 1.0 - max(dot(normal, viewDir), 0.0);
  rim = pow(rim, 3.0) * 0.15;

  vec3 color = vColor * lighting + spec + rim * vColor;

  // Selected: full opacity + brighter
  float opacity = mix(vOpacity * uBrightness, 1.0, vIsSelected);
  vec3 finalColor = mix(color, vColor * lighting * 1.8 + spec * 2.0, vIsSelected);

  gl_FragColor = vec4(finalColor, opacity);
}
