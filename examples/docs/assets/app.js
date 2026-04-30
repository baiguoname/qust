const search = document.querySelector("#site-search");
const navLinks = Array.from(document.querySelectorAll(".nav-link"));

if (search) {
  search.addEventListener("input", () => {
    const value = search.value.trim().toLowerCase();
    navLinks.forEach((link) => {
      const haystack = `${link.textContent} ${link.getAttribute("href")}`.toLowerCase();
      link.dataset.hidden = value && !haystack.includes(value) ? "true" : "false";
    });
  });
}

document.querySelectorAll(".code-block").forEach((block) => {
  const pre = block.querySelector("pre");
  const title = block.querySelector(".code-title");
  if (!pre || !title) return;
  const button = document.createElement("button");
  button.className = "copy";
  button.type = "button";
  button.textContent = "复制";
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(pre.innerText);
      button.textContent = "已复制";
      setTimeout(() => {
        button.textContent = "复制";
      }, 1100);
    } catch {
      button.textContent = "复制失败";
      setTimeout(() => {
        button.textContent = "复制";
      }, 1100);
    }
  });
  title.appendChild(button);
});

const headings = Array.from(document.querySelectorAll("main h2, main h3"));
const toc = document.querySelector("#page-toc");

if (toc) {
  const usedIds = new Set(Array.from(document.querySelectorAll("[id]")).map((node) => node.id));
  const slugify = (text) =>
    text
      .trim()
      .toLowerCase()
      .replace(/<[^>]+>/g, "")
      .replace(/[`"'()/.]/g, "")
      .replace(/\s+/g, "-")
      .replace(/[^\p{Letter}\p{Number}_-]+/gu, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 80) || "section";

  const ensureHeadingId = (heading) => {
    if (heading.id) return heading.id;
    const section = heading.closest("section[id]");
    let base =
      heading.tagName === "H2" && section
        ? section.id
        : `${section ? section.id : "section"}-${slugify(heading.textContent)}`;
    let id = base;
    let index = 2;
    while (usedIds.has(id)) {
      id = `${base}-${index++}`;
    }
    heading.id = id;
    usedIds.add(id);
    return id;
  };

  headings.forEach((heading) => {
    const id = ensureHeadingId(heading);
    const link = document.createElement("a");
    link.href = `#${id}`;
    link.textContent = heading.textContent;
    link.className = heading.tagName === "H3" ? "depth-3" : "depth-2";
    toc.appendChild(link);
  });
}
