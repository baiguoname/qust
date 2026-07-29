const canvas = document.querySelector(".market-canvas");
const ctx = canvas ? canvas.getContext("2d") : null;
let width = 0;
let height = 0;
let dpr = 1;
let frame = 0;
let particles = [];

function resizeCanvas() {
  if (!canvas || !ctx) return;
  dpr = Math.min(window.devicePixelRatio || 1, 2);
  width = window.innerWidth;
  height = window.innerHeight;
  canvas.width = Math.floor(width * dpr);
  canvas.height = Math.floor(height * dpr);
  canvas.style.width = `${width}px`;
  canvas.style.height = `${height}px`;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  particles = Array.from({ length: Math.min(56, Math.max(24, Math.floor(width / 26))) }, (_, index) => ({
    x: (index * 137.5) % width,
    y: (index * 83.7) % height,
    r: 1 + (index % 4) * 0.45,
    s: 0.12 + (index % 7) * 0.025,
    c: index % 3,
  }));
}

function drawMarketLines() {
  if (!canvas || !ctx) return;
  ctx.clearRect(0, 0, width, height);
  ctx.globalCompositeOperation = "lighter";
  const lines = [
    { color: "rgba(40,216,184,0.2)", y: 0.30, amp: 38, speed: 0.007 },
    { color: "rgba(105,167,255,0.16)", y: 0.52, amp: 52, speed: 0.005 },
    { color: "rgba(247,189,85,0.13)", y: 0.70, amp: 32, speed: 0.009 },
  ];
  for (const line of lines) {
    ctx.beginPath();
    for (let x = 0; x <= width; x += 12) {
      const y =
        height * line.y +
        Math.sin(x * 0.008 + frame * line.speed) * line.amp +
        Math.cos(x * 0.017 + frame * line.speed * 1.6) * line.amp * 0.34;
      if (x === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = line.color;
    ctx.lineWidth = 1.3;
    ctx.stroke();
  }
  for (const p of particles) {
    p.x += p.s;
    p.y += Math.sin(frame * 0.01 + p.x * 0.006) * 0.035;
    if (p.x > width + 12) p.x = -12;
    const colors = ["rgba(40,216,184,0.42)", "rgba(105,167,255,0.32)", "rgba(247,189,85,0.28)"];
    ctx.beginPath();
    ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
    ctx.fillStyle = colors[p.c];
    ctx.fill();
  }
  ctx.globalCompositeOperation = "source-over";
  frame += 1;
  requestAnimationFrame(drawMarketLines);
}

resizeCanvas();
drawMarketLines();
window.addEventListener("resize", resizeCanvas);
