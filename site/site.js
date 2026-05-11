const canvas = document.querySelector(".market-canvas");
const ctx = canvas.getContext("2d");
let width = 0;
let height = 0;
let dpr = 1;
let frame = 0;

function resizeCanvas() {
  dpr = Math.min(window.devicePixelRatio || 1, 2);
  width = window.innerWidth;
  height = window.innerHeight;
  canvas.width = Math.floor(width * dpr);
  canvas.height = Math.floor(height * dpr);
  canvas.style.width = `${width}px`;
  canvas.style.height = `${height}px`;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}

function drawMarketLines() {
  ctx.clearRect(0, 0, width, height);
  ctx.globalCompositeOperation = "lighter";

  const lines = [
    { color: "rgba(32,214,177,0.26)", y: 0.28, amp: 42, speed: 0.018 },
    { color: "rgba(91,140,255,0.2)", y: 0.44, amp: 58, speed: 0.014 },
    { color: "rgba(245,184,75,0.18)", y: 0.62, amp: 36, speed: 0.021 },
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
    ctx.lineWidth = 1.4;
    ctx.stroke();
  }

  ctx.globalCompositeOperation = "source-over";
  frame += 1;
  requestAnimationFrame(drawMarketLines);
}

resizeCanvas();
drawMarketLines();
window.addEventListener("resize", resizeCanvas);
