from __future__ import annotations

import argparse
import base64
import html
import json
import mimetypes
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

import nbformat
from bs4 import BeautifulSoup
from nbconvert import HTMLExporter, MarkdownExporter


ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parent
TUTORIAL_DIR = PROJECT / "examples" / "notebooks" / "tutorial"
BLOG_DIR = PROJECT / "examples" / "notebooks" / "blog"
NOTEBOOK_MD_DIR = PROJECT / "examples" / "notebooks_md"
STATIC_RENDER_DIR = ROOT / "assets" / "notebook-renders"
PROJECT_URL = "https://baiguoname.github.io/qust/site"
GIT_URL = "https://github.com/baiguoname/qust"


@dataclass(frozen=True)
class NotebookPage:
    source: Path
    title: str
    short_title: str
    description: str
    slug: str
    href: str
    cells: int
    outputs: int
    iframes: int


def slugify(path: Path) -> str:
    slug = path.stem.lower()
    slug = re.sub(r"[^\w]+", "-", slug, flags=re.UNICODE).strip("-")
    if not slug:
        slug = "notebook"
    return f"{slug}.html"


def natural_key(path: Path) -> list[object]:
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", path.stem)]


def compact_title(title: str, source: Path) -> str:
    short_title = re.sub(r"^\d+[\._\-\s]*", "", source.stem).strip()
    short_title = re.sub(r"[_\-]+", " ", short_title).strip()
    if not short_title:
        short_title = re.split(r"[：:]", title, maxsplit=1)[0].strip()
        short_title = re.sub(r"\s+with\s+qust$", "", short_title, flags=re.IGNORECASE)
    return short_title if len(short_title) <= 28 else f"{short_title[:26]}..."


def notebook_title(nb: nbformat.NotebookNode, fallback: str) -> str:
    for cell in nb.cells:
        if cell.cell_type == "markdown":
            for line in str(cell.source).splitlines():
                text = line.strip()
                if text.startswith("# "):
                    return text[2:].strip()
    return fallback


def notebook_description(nb: nbformat.NotebookNode) -> str:
    for cell in nb.cells:
        if cell.cell_type != "markdown":
            continue
        blocks = re.split(r"\n\s*\n", str(cell.source))
        for block in blocks:
            text = re.sub(r"^#+\s*", "", block.strip())
            text = re.sub(r"\*\*|`", "", text)
            if text and not text.startswith("#"):
                return text[:96]
    return "从 notebook 转换而来的网页版本，保留正文、代码和当前保存的输出。"


def notebook_stats(nb: nbformat.NotebookNode) -> tuple[int, int, int]:
    outputs = 0
    iframes = 0
    for cell in nb.cells:
        if cell.cell_type != "code":
            continue
        for output in cell.get("outputs", []):
            outputs += 1
            html_data = "".join(output.get("data", {}).get("text/html", ""))
            if "<iframe" in html_data:
                iframes += 1
    return len(nb.cells), outputs, iframes


def load_pages(kind: str, source_dir: Path) -> list[NotebookPage]:
    pages: list[NotebookPage] = []
    for path in sorted(source_dir.glob("*.ipynb"), key=natural_key):
        nb = nbformat.read(path, as_version=4)
        title = notebook_title(nb, path.stem)
        cells, outputs, iframes = notebook_stats(nb)
        slug = slugify(path)
        pages.append(
            NotebookPage(
                source=path,
                title=title,
                short_title=compact_title(title, path),
                description=notebook_description(nb),
                slug=slug,
                href=f"{kind}/{slug}",
                cells=cells,
                outputs=outputs,
                iframes=iframes,
            )
        )
    return pages


def nav(depth: int = 0) -> str:
    prefix = "../" * depth
    docs = f"{prefix}../examples/docs/index.html" if depth == 0 else f"{prefix}../examples/docs/index.html"
    return f"""
      <header class="topbar">
        <a class="brand" href="{prefix}index.html" aria-label="qust 首页">
          <span class="brand-mark">q</span>
          <span class="brand-text">qust</span>
        </a>
        <nav class="nav" aria-label="主导航">
          <a href="{prefix}tutorial.html">使用教程</a>
          <a href="{prefix}blog.html">Blog</a>
          <a href="{docs}">Doc</a>
          <a href="{prefix}service.html">服务</a>
          <a href="{PROJECT_URL}">项目地址</a>
          <a href="{GIT_URL}">git地址</a>
        </nav>
      </header>
    """


def html_shell(title: str, body: str, *, depth: int = 0, page_class: str = "") -> str:
    prefix = "../" * depth
    return f"""<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="description" content="qust 高性能计算引擎：DataFrame、流计算、量化研究、ClickHouse 与高性能算子。">
    <title>{html.escape(title)}</title>
    <link rel="stylesheet" href="{prefix}styles.css">
  </head>
  <body class="{page_class}">
    <canvas class="market-canvas" aria-hidden="true"></canvas>
{nav(depth)}
{body}
    <script src="{prefix}site.js"></script>
  </body>
</html>
"""


def card_grid(pages: list[NotebookPage], kind_title: str, intro: str, href_prefix: str) -> str:
    cards = []
    for page in pages:
        cards.append(
            f"""
          <a class="doc-card" href="{html.escape(page.href)}">
            <span>{html.escape(kind_title)}</span>
            <h2 title="{html.escape(page.title)}">{html.escape(page.short_title)}</h2>
            <p>{html.escape(page.description)}</p>
            <div class="doc-meta">
              <b>{page.cells} cells</b>
              <b>{page.outputs} outputs</b>
              <b>{page.iframes} monitors</b>
            </div>
          </a>
            """
        )
    return f"""
    <main class="page-shell">
      <section class="page-hero compact">
        <p class="eyebrow">{html.escape(href_prefix.upper())}</p>
        <h1>{html.escape(kind_title)}</h1>
        <p>{html.escape(intro)}</p>
      </section>
      <section class="doc-grid" aria-label="{html.escape(kind_title)}">
        {''.join(cards)}
      </section>
    </main>
    """


def static_render_name(collection: str, slug: str, index: int) -> str:
    return f"{collection}-{Path(slug).stem}-{index + 1:02d}.png"


def resolve_static_render_path(collection: str, slug: str, index: int) -> Path:
    expected = STATIC_RENDER_DIR / static_render_name(collection, slug, index)
    if expected.exists():
        return expected

    slug_stem = Path(slug).stem
    suffix = f"-{index + 1:02d}.png"
    number_match = re.match(r"^(\d+)(?:-|$)", slug_stem)
    patterns: list[str] = []
    if number_match:
        patterns.append(f"{collection}-{number_match.group(1)}-*{suffix}")
    patterns.append(f"{collection}-{slug_stem}*{suffix}")
    for pattern in patterns:
        matches = sorted(STATIC_RENDER_DIR.glob(pattern))
        if matches:
            return matches[0]
    return expected


def iframe_height(iframe) -> int:
    value = iframe.get("height") or iframe.get("style", "")
    match = re.search(r"(\d+)", str(value))
    if not match:
        return 640
    return max(360, min(int(match.group(1)), 1200))


def clean_notebook_html(body: str, collection: str, slug: str) -> str:
    soup = BeautifulSoup(body, "html.parser")
    for tag in soup.select(".prompt, .anchor-link"):
        tag.decompose()
    for table in soup.find_all("table"):
        table["class"] = "dataframe-table"
    for pre in soup.find_all("pre"):
        classes = set(pre.get("class", []))
        classes.add("code-block")
        pre["class"] = sorted(classes)
    for idx, iframe in enumerate(soup.find_all("iframe")):
        image_path = resolve_static_render_path(collection, slug, idx)
        filename = image_path.name
        src = iframe.get("src", "")
        figure = soup.new_tag("figure")
        figure["class"] = "monitor-static"
        if image_path.exists():
            image = soup.new_tag("img")
            image["src"] = f"../assets/notebook-renders/{filename}"
            image["alt"] = f"{Path(slug).stem} monitor output {idx + 1}"
            image["loading"] = "lazy"
            figure.append(image)
            caption = soup.new_tag("figcaption")
            caption.string = "notebook 输出：monitor 静态图"
            figure.append(caption)
        else:
            figure["class"] = "monitor-static missing"
            title = soup.new_tag("strong")
            title.string = "此 monitor 输出没有静态截图"
            figure.append(title)
            text = soup.new_tag("p")
            text.string = "需要重新执行对应 notebook 后，再运行站点构建脚本生成静态图。"
            figure.append(text)
            if src:
                link = soup.new_tag("a", href=src)
                link.string = "查看原始 monitor 输出"
                figure.append(link)
        iframe.replace_with(figure)
    return str(soup)


def write_notebook_page(page: NotebookPage, collection: str, all_pages: list[NotebookPage]) -> None:
    nb = nbformat.read(page.source, as_version=4)
    exporter = HTMLExporter(template_name="basic")
    raw_body, _ = exporter.from_notebook_node(nb)
    article = clean_notebook_html(raw_body, collection, page.slug)
    links = []
    for item in all_pages:
        active = " active" if item.slug == page.slug else ""
        links.append(f'<a class="article-link{active}" href="{html.escape(item.slug)}">{html.escape(item.short_title)}</a>')
    body = f"""
    <main class="article-layout">
      <aside class="article-sidebar">
        <a class="back-link" href="../{collection}.html">返回目录</a>
        <div class="article-list">
          {''.join(links)}
        </div>
      </aside>
      <article class="notebook-article">
        {article}
      </article>
    </main>
    """
    out_dir = ROOT / collection
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / page.slug).write_text(
        html_shell(f"{page.title} | qust", body, depth=1, page_class="article-page"),
        encoding="utf-8",
    )


def bytes_to_data_uri(data: bytes, filename: str) -> str:
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type is None:
        mime_type = "application/octet-stream"
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def image_path_to_data_uri(path: Path) -> str:
    return bytes_to_data_uri(path.read_bytes(), path.name)


def clean_markdown_monitor_outputs(body: str, collection: str, slug: str) -> str:
    iframe_index = 0

    def replace_iframe(match: re.Match[str]) -> str:
        nonlocal iframe_index
        image_path = resolve_static_render_path(collection, slug, iframe_index)
        iframe_index += 1
        if image_path.exists():
            return f"\n\n![monitor 输出]({image_path_to_data_uri(image_path)})\n\n"
        return "\n\n> 此 monitor 输出没有静态截图。重新执行 notebook 后，再运行站点构建脚本生成静态图。\n\n"

    return re.sub(r"<iframe\b.*?</iframe>", replace_iframe, body, flags=re.I | re.S)


def inline_markdown_output_images(body: str, outputs: dict[str, object]) -> str:
    output_data: dict[str, str] = {}
    for filename, data in outputs.items():
        raw = data if isinstance(data, bytes) else str(data).encode("utf-8")
        data_uri = bytes_to_data_uri(raw, filename)
        output_data[filename] = data_uri
        output_data[unquote(filename)] = data_uri

    def replace_image(match: re.Match[str]) -> str:
        alt = match.group(1)
        target = match.group(2).strip()
        quote = ""
        if (target.startswith('"') and target.endswith('"')) or (target.startswith("'") and target.endswith("'")):
            quote = target[0]
            target = target[1:-1].strip()
        data_uri = output_data.get(target) or output_data.get(unquote(target))
        if data_uri is None:
            return match.group(0)
        wrapped = f"{quote}{data_uri}{quote}" if quote else data_uri
        return f"![{alt}]({wrapped})"

    return re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", replace_image, body)


def write_notebook_markdown(page: NotebookPage, collection: str) -> None:
    nb = nbformat.read(page.source, as_version=4)
    output_files_dir = f"{page.source.stem}_files"
    exporter = MarkdownExporter()
    body, resources = exporter.from_notebook_node(
        nb,
        resources={"output_files_dir": output_files_dir},
    )
    body = clean_markdown_monitor_outputs(body, collection, page.slug)
    body = inline_markdown_output_images(body, resources.get("outputs", {}))

    out_dir = NOTEBOOK_MD_DIR / collection
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{page.source.stem}.md").write_text(body, encoding="utf-8")


def collect_monitor_jobs(pages: list[NotebookPage], collection: str) -> list[dict[str, object]]:
    jobs: list[dict[str, object]] = []
    exporter = HTMLExporter(template_name="basic")
    for page in pages:
        nb = nbformat.read(page.source, as_version=4)
        raw_body, _ = exporter.from_notebook_node(nb)
        soup = BeautifulSoup(raw_body, "html.parser")
        for idx, iframe in enumerate(soup.find_all("iframe")):
            src = iframe.get("src")
            if not src:
                continue
            filename = static_render_name(collection, page.slug, idx)
            jobs.append(
                {
                    "url": src,
                    "out": str(STATIC_RENDER_DIR / filename),
                    "height": iframe_height(iframe),
                    "title": page.title,
                }
            )
    return jobs


def capture_static_monitor_images(
    tutorial: list[NotebookPage],
    blog: list[NotebookPage],
    *,
    force: bool = False,
) -> None:
    jobs = collect_monitor_jobs(tutorial, "tutorial") + collect_monitor_jobs(blog, "blog")
    STATIC_RENDER_DIR.mkdir(parents=True, exist_ok=True)
    pending = [job for job in jobs if force or not Path(str(job["out"])).exists()]
    if not pending:
        print(f"static monitor images already available: {len(jobs)}")
        return

    script = r"""
const fs = require("fs");
const { chromium } = require("/root/otters/node_modules/playwright");

const jobs = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));

(async () => {
  const browser = await chromium.launch({ headless: true });
  const results = [];
  for (const job of jobs) {
    const height = Math.max(420, Math.min(Number(job.height || 640), 1200));
    const page = await browser.newPage({
      viewport: { width: 1280, height },
      deviceScaleFactor: 1,
    });
    try {
      await page.goto(job.url, { waitUntil: "domcontentloaded", timeout: 6000 });
      await page.waitForTimeout(1800);
      await page.screenshot({ path: job.out, fullPage: false });
      results.push({ ok: true, out: job.out, url: job.url });
    } catch (error) {
      results.push({ ok: false, out: job.out, url: job.url, error: String(error.message || error) });
    } finally {
      await page.close();
    }
  }
  await browser.close();
  console.log(JSON.stringify(results, null, 2));
})();
"""
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as jobs_file:
        json.dump(pending, jobs_file, ensure_ascii=False)
        jobs_path = jobs_file.name
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as script_file:
        script_file.write(script)
        script_path = script_file.name
    try:
        proc = subprocess.run(
            ["node", script_path, jobs_path],
            cwd="/root/otters",
            text=True,
            check=False,
            capture_output=True,
        )
    finally:
        Path(jobs_path).unlink(missing_ok=True)
        Path(script_path).unlink(missing_ok=True)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        raise RuntimeError("static monitor screenshot capture failed")
    results = json.loads(proc.stdout or "[]")
    ok = sum(1 for item in results if item.get("ok"))
    failed = [item for item in results if not item.get("ok")]
    print(f"captured static monitor images: {ok}/{len(pending)}")
    if failed:
        print(f"monitor screenshots skipped: {len(failed)}")
        for item in failed[:20]:
            print(f"- {item.get('url')}: {item.get('error')}")


def write_index() -> None:
    bubbles = [
        ("Polars", "表达式互操作"),
        ("流计算", "状态持续推进"),
        ("DuckDB", "分析型 SQL"),
        ("ClickHouse", "远程列式数据"),
        ("DataFrame", "批量 / 流式同源"),
        ("Arrow", "零拷贝列式内存"),
        ("Rust", "高性能执行器"),
        ("Python", "研究端表达式"),
        ("Monitor", "交互图表输出"),
        ("Optuna", "参数寻优"),
        ("Batch", "批处理算子"),
        ("QMT", "实盘联调"),
    ]
    bubble_html = "\n".join(
        f'<span class="bubble" style="--i:{i};"><b>{html.escape(name)}</b><em>{html.escape(desc)}</em></span>'
        for i, (name, desc) in enumerate(bubbles)
    )
    body = f"""
    <main class="home">
      <section class="home-hero" aria-label="qust">
        <div class="bubble-field" aria-hidden="true">
          {bubble_html}
        </div>
        <div class="home-copy">
          <h1>qust</h1>
          <p>面向 DataFrame、流计算与量化研究的高性能计算引擎。</p>
        </div>
      </section>
    </main>
    """
    (ROOT / "index.html").write_text(html_shell("qust | 高性能计算引擎", body, page_class="home-page"), encoding="utf-8")


def write_service() -> None:
    body = """
    <main class="page-shell service-page">
      <section class="service-hero">
        <div>
          <p class="eyebrow">SERVICE</p>
          <h1>qust 服务与源码交付</h1>
          <p>围绕高性能算子开发、量化平台搭建、源码提供、私有部署、数据源接入和实盘联调，把研究脚本升级成可维护的工程资产。</p>
        </div>
        <div class="service-focus">
          <strong>核心服务</strong>
          <span>高性能算子开发</span>
          <span>量化平台搭建</span>
          <span>源码提供与私有部署</span>
        </div>
      </section>

      <section class="service-grid">
        <article>
          <span>01</span>
          <h2>高性能算子开发</h2>
          <p>把 Python 研究逻辑下沉到 Rust/Arrow 执行路径，适合 rolling、rank、group、over、tick/kline、回测、风控和交易状态类算子。</p>
          <ul>
            <li>梳理输入/输出 schema、空值和流式语义。</li>
            <li>实现 Rust 原生执行器、Python namespace 和文档示例。</li>
            <li>补齐小样本、批量、流式和性能测试。</li>
          </ul>
        </article>
        <article>
          <span>02</span>
          <h2>量化平台搭建</h2>
          <p>围绕行情、因子、策略、回测、参数、监控和报告搭建统一工作流，让研究员用表达式沉淀策略资产。</p>
          <ul>
            <li>设计数据源接入、缓存、分块计算和结果归档。</li>
            <li>建立策略模板、指标库、回测评价和组合分析。</li>
            <li>把 notebook、脚本、服务进程和监控页面组织起来。</li>
          </ul>
        </article>
        <article>
          <span>03</span>
          <h2>源码提供与私有部署</h2>
          <p>按团队环境交付源码、构建脚本、部署说明和二次开发规范，方便在内网或专有服务器里长期维护。</p>
          <ul>
            <li>支持闭源私有交付、版本升级和接口冻结。</li>
            <li>提供 Python/Rust 构建、wheel、Docker 和 CI 建议。</li>
            <li>按团队权限、数据安全和运维方式定制部署。</li>
          </ul>
        </article>
        <article>
          <span>04</span>
          <h2>数据源与远程计算</h2>
          <p>接入 Parquet、ClickHouse、LazyFrame、流式数据和自定义 datasource，减少研究代码里的 IO 分叉。</p>
          <ul>
            <li>ClickHouse schema、Date/DateTime、字符串类型和 Arrow 类型适配。</li>
            <li>大数据分块读取、谓词下推和流式执行链路。</li>
            <li>数据质量检查、抽样校验和异常定位。</li>
          </ul>
        </article>
        <article>
          <span>05</span>
          <h2>Monitor 与研究 UI</h2>
          <p>把价格、信号、PnL、分布、参数、debug trace 和交互 callback 做成研究员能直接使用的页面。</p>
          <ul>
            <li>定制图表、联动选择、参数面板和调试视图。</li>
            <li>把 notebook 输出升级成稳定的研究 dashboard。</li>
            <li>保留表达式链路，避免图表和计算逻辑割裂。</li>
          </ul>
        </article>
        <article>
          <span>06</span>
          <h2>实盘联调与策略服务</h2>
          <p>把离线表达式接到模拟盘、实盘 API 或内部交易系统，重点处理信号时序、持仓状态、风控和日志回放。</p>
          <ul>
            <li>研究表达式、回测、模拟盘和实盘调用保持一致。</li>
            <li>接入 QMT 等交易接口，拆分 API 层和策略层。</li>
            <li>提供运行手册、异常日志、停止/暂停和恢复机制。</li>
          </ul>
        </article>
      </section>

      <section class="service-process">
        <h2>合作流程</h2>
        <div>
          <article><b>1</b><span>梳理场景</span><p>明确数据、算子、性能、部署和策略运行方式。</p></article>
          <article><b>2</b><span>做最小可运行版本</span><p>先跑通核心表达式、结果输出和验收样例。</p></article>
          <article><b>3</b><span>扩展工程链路</span><p>补齐测试、文档、监控、部署和团队二次开发接口。</p></article>
          <article><b>4</b><span>维护升级</span><p>持续处理算子扩展、性能瓶颈、数据源变化和版本升级。</p></article>
        </div>
      </section>

      <section class="contact-panel">
        <p class="eyebrow">CONTACT</p>
        <h2>联系服务支持</h2>
        <p>适合沟通：算子开发、源码交付、ClickHouse 接入、量化平台、实盘联调、Monitor 面板和策略研究工作流。</p>
        <strong>微信：aruster</strong>
      </section>
    </main>
    """
    (ROOT / "service.html").write_text(html_shell("服务 | qust", body, page_class="service"), encoding="utf-8")


def write_css() -> None:
    (ROOT / "styles.css").write_text(
        r''':root {
  color-scheme: dark;
  --bg: #080a0f;
  --ink: #f7fbff;
  --muted: #a7b3c2;
  --line: rgba(255, 255, 255, 0.12);
  --panel: rgba(16, 21, 30, 0.82);
  --panel-strong: #111923;
  --teal: #28d8b8;
  --blue: #69a7ff;
  --amber: #f7bd55;
  --rose: #ff7b72;
  --green: #8ee88e;
  --violet: #b79cff;
  --radius: 8px;
  --font-sans: "MiSans", "HarmonyOS Sans SC", "PingFang SC", "Noto Sans SC", "Microsoft YaHei UI", "Microsoft YaHei", "Segoe UI", Arial, sans-serif;
  --font-mono: "JetBrains Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace;
}

* { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  min-height: 100vh;
  margin: 0;
  overflow-x: hidden;
  background: linear-gradient(180deg, #080a0f 0%, #0b1018 48%, #080a0f 100%);
  color: var(--ink);
  font-family: var(--font-sans);
  line-height: 1.72;
}
body::before {
  position: fixed;
  inset: 0;
  z-index: -3;
  background:
    linear-gradient(rgba(255,255,255,0.035) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255,255,255,0.03) 1px, transparent 1px);
  background-size: 72px 72px;
  mask-image: linear-gradient(180deg, #000 0%, transparent 82%);
  content: "";
}
a { color: inherit; text-decoration: none; }
img { max-width: 100%; display: block; }
code, pre { font-family: var(--font-mono); }

.market-canvas {
  position: fixed;
  inset: 0;
  z-index: -2;
  width: 100%;
  height: 100%;
  opacity: 0.7;
  pointer-events: none;
}

.topbar {
  position: sticky;
  top: 0;
  z-index: 20;
  display: grid;
  grid-template-columns: auto 1fr;
  align-items: center;
  min-height: 64px;
  gap: 28px;
  padding: 10px clamp(20px, 4vw, 58px);
  border-bottom: 1px solid var(--line);
  background: rgba(8, 10, 15, 0.76);
  backdrop-filter: blur(18px);
}
.brand {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  font-weight: 760;
}
.brand-mark {
  display: grid;
  width: 36px;
  height: 36px;
  place-items: center;
  border: 1px solid rgba(40, 216, 184, 0.48);
  border-radius: var(--radius);
  color: var(--teal);
}
.brand-text { font-size: 18px; }
.nav {
  display: flex;
  justify-content: flex-end;
  gap: clamp(18px, 3vw, 36px);
  color: #c9d4e1;
  font-size: 14px;
  font-weight: 680;
}
.nav a:hover { color: var(--teal); }

.home { min-height: calc(100vh - 64px); }
.home-hero {
  position: relative;
  display: grid;
  min-height: calc(100vh - 64px);
  place-items: center;
  padding: 64px 20px;
  overflow: hidden;
}
.home-hero::before,
.home-hero::after {
  position: absolute;
  inset: 10% 9%;
  z-index: 0;
  border: 1px solid rgba(40, 216, 184, 0.13);
  border-radius: 50%;
  transform: rotate(-8deg);
  content: "";
}
.home-hero::after {
  inset: 18% 18%;
  border-color: rgba(105, 167, 255, 0.12);
  transform: rotate(12deg);
}
.home-copy {
  position: relative;
  z-index: 3;
  text-align: center;
  text-shadow: 0 24px 80px rgba(0, 0, 0, 0.62);
}
.home h1 {
  margin: 0;
  font-size: clamp(76px, 16vw, 220px);
  font-weight: 760;
  line-height: 0.92;
  letter-spacing: 0;
}
.home p {
  max-width: 760px;
  margin: 30px auto 0;
  color: #d4deea;
  font-size: clamp(19px, 2.2vw, 30px);
  font-weight: 520;
}
.bubble-field {
  position: absolute;
  inset: 0;
  z-index: 2;
}
.bubble-field::before,
.bubble-field::after {
  position: absolute;
  width: 46vw;
  height: 46vw;
  border: 1px solid rgba(255, 255, 255, 0.09);
  border-radius: 50%;
  background:
    radial-gradient(circle at center, rgba(40,216,184,0.08), transparent 56%),
    repeating-conic-gradient(from 20deg, rgba(255,255,255,0.07) 0deg 1deg, transparent 1deg 18deg);
  mask-image: radial-gradient(circle, #000 0 53%, transparent 68%);
  animation: rotate-field 90s linear infinite;
  content: "";
}
.bubble-field::before {
  left: -12vw;
  top: 22vh;
}
.bubble-field::after {
  right: -14vw;
  bottom: 10vh;
  width: 38vw;
  height: 38vw;
  animation-direction: reverse;
  animation-duration: 120s;
}
.bubble {
  position: absolute;
  display: grid;
  min-width: 162px;
  min-height: 106px;
  place-items: center;
  padding: 20px 24px;
  border: 1px solid rgba(255,255,255,0.22);
  border-radius: 999px;
  background:
    linear-gradient(145deg, rgba(255,255,255,0.11), rgba(255,255,255,0.02)),
    radial-gradient(circle at 28% 18%, rgba(255,255,255,0.24), transparent 36%),
    rgba(14, 20, 31, 0.72);
  box-shadow:
    0 28px 90px rgba(0,0,0,0.34),
    0 0 42px color-mix(in srgb, var(--bubble-color, var(--teal)) 20%, transparent),
    inset 0 1px 0 rgba(255,255,255,0.16);
  transform: translate3d(var(--x), var(--y), 0);
  animation: float-bubble calc(38s + var(--i) * 1.4s) ease-in-out infinite alternate;
  animation-delay: calc(var(--i) * -2.4s);
  backdrop-filter: blur(12px);
}
.bubble b {
  color: var(--bubble-color, var(--teal));
  font-size: 22px;
  font-weight: 780;
  line-height: 1.15;
}
.bubble em {
  color: #aab7c7;
  font-size: 13px;
  font-weight: 560;
  font-style: normal;
  white-space: nowrap;
}
.bubble:nth-child(1) { --x: 8vw; --y: 19vh; --bubble-color: var(--teal); }
.bubble:nth-child(2) { --x: 19vw; --y: 70vh; --bubble-color: var(--green); }
.bubble:nth-child(3) { --x: 70vw; --y: 18vh; --bubble-color: var(--amber); }
.bubble:nth-child(4) { --x: 80vw; --y: 64vh; --bubble-color: var(--blue); }
.bubble:nth-child(5) { --x: 42vw; --y: 13vh; --bubble-color: var(--rose); }
.bubble:nth-child(6) { --x: 58vw; --y: 74vh; --bubble-color: var(--violet); }
.bubble:nth-child(7) { --x: 11vw; --y: 48vh; --bubble-color: var(--rose); }
.bubble:nth-child(8) { --x: 82vw; --y: 42vh; --bubble-color: var(--green); }
.bubble:nth-child(9) { --x: 28vw; --y: 30vh; --bubble-color: var(--blue); }
.bubble:nth-child(10) { --x: 63vw; --y: 35vh; --bubble-color: var(--teal); }
.bubble:nth-child(11) { --x: 34vw; --y: 76vh; --bubble-color: var(--amber); }
.bubble:nth-child(12) { --x: 51vw; --y: 56vh; --bubble-color: var(--violet); }
@keyframes float-bubble {
  0% { translate: -24px -16px; rotate: -2deg; }
  45% { translate: 18px 22px; rotate: 1.5deg; }
  100% { translate: -8px 36px; rotate: 3deg; }
}
@keyframes rotate-field {
  to { rotate: 360deg; }
}

.page-shell {
  width: min(1180px, calc(100% - 40px));
  margin: 0 auto;
  padding: 62px 0 84px;
}
.page-hero {
  padding: 42px 0 34px;
  border-bottom: 1px solid var(--line);
}
.page-hero.compact h1,
.service-hero h1 {
  margin: 0 0 14px;
  font-size: clamp(42px, 7vw, 82px);
  line-height: 1.04;
  letter-spacing: 0;
}
.page-hero p,
.service-hero p {
  max-width: 860px;
  margin: 0;
  color: #c9d4e0;
  font-size: 18px;
}
.eyebrow {
  margin: 0 0 12px;
  color: var(--teal);
  font-size: 12px;
  font-weight: 740;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
.doc-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 18px;
  padding-top: 28px;
}
.doc-card {
  min-height: 260px;
  padding: 22px;
  border: 1px solid var(--line);
  border-radius: var(--radius);
  background: var(--panel);
  transition: transform 170ms ease, border-color 170ms ease, background 170ms ease;
}
.doc-card:hover {
  transform: translateY(-4px);
  border-color: rgba(40,216,184,0.5);
  background: rgba(19, 27, 38, 0.94);
}
.doc-card span {
  color: var(--amber);
  font-size: 12px;
  font-weight: 760;
  text-transform: uppercase;
}
.doc-card h2 {
  margin: 12px 0 10px;
  font-size: 21px;
  line-height: 1.32;
}
.doc-card p {
  color: #afbdcc;
  font-size: 14px;
}
.doc-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 18px;
}
.doc-meta b {
  padding: 5px 8px;
  border: 1px solid rgba(255,255,255,0.12);
  border-radius: 999px;
  color: #d6e0ec;
  font-size: 12px;
  font-weight: 640;
}

.article-layout {
  display: grid;
  grid-template-columns: minmax(190px, 250px) minmax(0, 920px);
  gap: 34px;
  width: min(1240px, calc(100% - 36px));
  margin: 0 auto;
  padding: 34px 0 80px;
}
.article-sidebar {
  position: sticky;
  top: 88px;
  align-self: start;
  max-height: calc(100vh - 110px);
  overflow: auto;
  padding-right: 8px;
}
.back-link,
.article-link {
  display: block;
  border-radius: var(--radius);
}
.back-link {
  margin-bottom: 14px;
  padding: 10px 12px;
  border: 1px solid rgba(40,216,184,0.34);
  color: var(--teal);
  font-weight: 720;
}
.article-list {
  display: grid;
  gap: 8px;
}
.article-link {
  padding: 10px 12px;
  color: #acb8c7;
  font-size: 13px;
  line-height: 1.42;
}
.article-link:hover,
.article-link.active {
  background: rgba(255,255,255,0.07);
  color: #fff;
}
.notebook-article {
  min-width: 0;
  padding: clamp(22px, 3vw, 44px);
  border: 1px solid var(--line);
  border-radius: var(--radius);
  background: rgba(12, 16, 23, 0.86);
}
.notebook-article h1 {
  margin: 0 0 22px;
  font-size: clamp(32px, 4.5vw, 52px);
  line-height: 1.12;
}
.notebook-article h2 {
  margin: 42px 0 14px;
  padding-top: 18px;
  border-top: 1px solid rgba(255,255,255,0.1);
  font-size: 24px;
}
.notebook-article h3 { margin: 28px 0 10px; font-size: 19px; }
.notebook-article p,
.notebook-article li { color: #d4deea; }
.notebook-article a { color: var(--teal); }
.notebook-article pre {
  overflow: auto;
  margin: 18px 0 22px;
  padding: 18px 20px;
  border: 1px solid rgba(105,167,255,0.2);
  border-radius: var(--radius);
  background:
    linear-gradient(180deg, rgba(13,18,27,0.98), rgba(4,7,12,0.98)),
    #05070b;
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.06), 0 18px 50px rgba(0,0,0,0.22);
  color: #dce7f4;
  font-size: 14px;
  line-height: 1.72;
}
.notebook-article code {
  color: #f6d184;
  font-size: 0.92em;
}
.notebook-article pre code { color: inherit; }
.notebook-article pre .kn,
.notebook-article pre .k,
.notebook-article pre .ow { color: #ff7b72; }
.notebook-article pre .nn,
.notebook-article pre .nc,
.notebook-article pre .nf { color: #7ee787; }
.notebook-article pre .n,
.notebook-article pre .bp { color: #dce7f4; }
.notebook-article pre .s,
.notebook-article pre .s1,
.notebook-article pre .s2 { color: #a5d6ff; }
.notebook-article pre .m,
.notebook-article pre .mi,
.notebook-article pre .mf { color: #f2cc60; }
.notebook-article pre .c,
.notebook-article pre .c1,
.notebook-article pre .ch { color: #8b949e; font-style: italic; }
.notebook-article pre .o,
.notebook-article pre .p { color: #c9d1d9; }
.notebook-article pre .nb { color: #d2a8ff; }
.cell { margin: 0 0 24px; }
.output_area,
.jp-RenderedHTMLCommon,
.rendered_html {
  max-width: 100%;
}
.output_area {
  overflow: auto;
  margin: 12px 0 20px;
  padding: 12px;
  border: 1px solid rgba(255,255,255,0.1);
  border-radius: var(--radius);
  background: rgba(255,255,255,0.045);
}
.dataframe-table {
  width: max-content;
  min-width: 100%;
  border-collapse: collapse;
  color: #e8eef7;
  font-size: 13px;
}
.dataframe-table th,
.dataframe-table td {
  padding: 7px 9px;
  border: 1px solid rgba(255,255,255,0.14);
}
.notebook-monitor {
  width: 100%;
  min-height: 560px;
  border: 1px solid rgba(255,255,255,0.12);
  border-radius: var(--radius);
  background: #05070b;
}
.monitor-static {
  margin: 16px 0 22px;
  padding: 12px;
  border: 1px solid rgba(105,167,255,0.2);
  border-radius: var(--radius);
  background: rgba(2, 5, 10, 0.9);
  box-shadow: 0 20px 60px rgba(0,0,0,0.26);
}
.monitor-static img {
  width: 100%;
  border-radius: 4px;
  background: #02050a;
}
.monitor-static figcaption {
  margin-top: 8px;
  color: #91a0b4;
  font-size: 12px;
}
.monitor-static.missing {
  min-height: 220px;
  display: grid;
  align-content: center;
  gap: 8px;
  padding: 28px;
  border-style: dashed;
  background: rgba(255, 189, 85, 0.08);
}
.monitor-static.missing strong { color: var(--amber); }
.monitor-static.missing p { margin: 0; color: #c7d2df; }
.monitor-static.missing a { color: var(--teal); }

.service-hero {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(260px, 360px);
  gap: 28px;
  align-items: end;
  padding: 48px 0 36px;
}
.service-focus {
  display: grid;
  gap: 10px;
  padding: 20px;
  border: 1px solid rgba(40,216,184,0.28);
  border-radius: var(--radius);
  background: rgba(14, 26, 28, 0.82);
}
.service-focus strong { color: var(--teal); font-size: 13px; }
.service-focus span { font-size: 18px; font-weight: 720; }
.service-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 18px;
  margin-top: 24px;
}
.service-grid article,
.service-process,
.contact-panel {
  border: 1px solid var(--line);
  border-radius: var(--radius);
  background: var(--panel);
}
.service-grid article {
  padding: 22px;
}
.service-grid article > span {
  color: var(--amber);
  font-weight: 760;
}
.service-grid h2 {
  margin: 12px 0 10px;
  font-size: 23px;
}
.service-grid p,
.service-grid li,
.service-process p,
.contact-panel p {
  color: #bfccd9;
}
.service-grid ul {
  margin: 14px 0 0;
  padding-left: 18px;
}
.service-process {
  margin-top: 22px;
  padding: 24px;
}
.service-process h2,
.contact-panel h2 { margin: 0 0 16px; font-size: 30px; }
.service-process > div {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 14px;
}
.service-process article {
  padding: 16px;
  border-radius: var(--radius);
  background: rgba(255,255,255,0.055);
}
.service-process b {
  display: grid;
  width: 30px;
  height: 30px;
  margin-bottom: 12px;
  place-items: center;
  border-radius: 50%;
  background: rgba(40,216,184,0.16);
  color: var(--teal);
}
.service-process span { display: block; font-weight: 760; }
.contact-panel {
  margin-top: 22px;
  padding: 28px;
}
.contact-panel strong {
  display: inline-block;
  margin-top: 6px;
  color: var(--teal);
  font-size: 24px;
}

@media (max-width: 980px) {
  .topbar { grid-template-columns: 1fr; gap: 12px; }
  .nav { justify-content: flex-start; overflow-x: auto; padding-bottom: 4px; }
  .doc-grid,
  .service-grid,
  .service-process > div { grid-template-columns: 1fr 1fr; }
  .article-layout { grid-template-columns: 1fr; }
  .article-sidebar { position: static; max-height: none; }
  .service-hero { grid-template-columns: 1fr; }
  .bubble { scale: 0.82; }
}
@media (max-width: 640px) {
  .doc-grid,
  .service-grid,
  .service-process > div { grid-template-columns: 1fr; }
  .home-hero { min-height: calc(100svh - 98px); padding: 42px 20px 76px; }
  .home-copy {
    width: min(100%, 350px);
    text-shadow: 0 10px 36px rgba(0,0,0,0.86);
  }
  .home h1 { font-size: clamp(86px, 31vw, 132px); }
  .home p { max-width: 340px; font-size: 18px; line-height: 1.48; }
  .bubble {
    min-width: 92px;
    min-height: 58px;
    padding: 11px 12px;
    opacity: 0.44;
    scale: 0.66;
  }
  .bubble em { display: none; }
  .bubble:nth-child(2),
  .bubble:nth-child(4),
  .bubble:nth-child(6),
  .bubble:nth-child(n+9) { display: none; }
  .bubble:nth-child(1) { --x: 3vw; --y: 18vh; }
  .bubble:nth-child(2) { --x: 6vw; --y: 72vh; }
  .bubble:nth-child(3) { --x: 62vw; --y: 20vh; }
  .bubble:nth-child(4) { --x: 58vw; --y: 70vh; }
  .bubble:nth-child(5) { --x: 34vw; --y: 12vh; }
  .bubble:nth-child(6) { --x: 40vw; --y: 80vh; }
  .bubble:nth-child(7) { --x: 1vw; --y: 46vh; }
  .bubble:nth-child(8) { --x: 66vw; --y: 46vh; }
  .notebook-article { padding: 18px; }
  .notebook-monitor { min-height: 440px; }
}
''',
        encoding="utf-8",
    )


def write_js() -> None:
    (ROOT / "site.js").write_text(
        r'''const canvas = document.querySelector(".market-canvas");
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
''',
        encoding="utf-8",
    )


def clean_generated_dirs() -> None:
    for path in (ROOT / "tutorial", ROOT / "blog", NOTEBOOK_MD_DIR):
        if path.exists():
            shutil.rmtree(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the qust static site from notebooks.")
    parser.add_argument(
        "--capture-monitor",
        action="store_true",
        help="Render saved notebook monitor iframe outputs into static PNG files.",
    )
    parser.add_argument(
        "--force-capture",
        action="store_true",
        help="Re-render monitor PNG files even when cached images already exist.",
    )
    args = parser.parse_args()
    clean_generated_dirs()
    tutorial = load_pages("tutorial", TUTORIAL_DIR)
    blog = load_pages("blog", BLOG_DIR)
    if args.capture_monitor:
        capture_static_monitor_images(tutorial, blog, force=args.force_capture)
    write_index()
    write_service()
    write_css()
    write_js()
    (ROOT / "tutorial.html").write_text(
        html_shell(
            "使用教程 | qust",
            card_grid(
                tutorial,
                "使用教程",
                "从安装、基础表达式、上下文、性能、monitor、策略分析、指标、参数优化到组合策略，系统展示 qust 的主要用法，并保留当前保存的输出。",
                "tutorial",
            ),
            page_class="listing",
        ),
        encoding="utf-8",
    )
    (ROOT / "blog.html").write_text(
        html_shell(
            "Blog | qust",
            card_grid(
                blog,
                "Blog",
                "围绕 Investopedia 技术分析文章、指标实现和完整策略回测的 notebook 网页版本，保留正文、代码、表格和 monitor 输出。",
                "blog",
            ),
            page_class="listing",
        ),
        encoding="utf-8",
    )
    for page in tutorial:
        write_notebook_page(page, "tutorial", tutorial)
        write_notebook_markdown(page, "tutorial")
    for page in blog:
        write_notebook_page(page, "blog", blog)
        write_notebook_markdown(page, "blog")
    print(
        f"generated {len(tutorial)} tutorial pages, "
        f"{len(blog)} blog pages, and markdown copies in {NOTEBOOK_MD_DIR}"
    )


if __name__ == "__main__":
    main()
