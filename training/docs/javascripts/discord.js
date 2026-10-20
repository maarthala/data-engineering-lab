// Discord invite on every page: a header button (extreme right) and a box under the right-hand Table of contents.
const DISCORD_URL = "https://discord.gg/sByPdSRFjf";

function discordLink(className, html) {
  const a = document.createElement("a");
  a.className = className;
  a.href = DISCORD_URL;
  a.target = "_blank";
  a.rel = "noopener";
  a.title = "Get help learning Data Engineering — join our Discord";
  a.innerHTML = html;
  return a;
}

document.addEventListener("DOMContentLoaded", () => {
  const header = document.querySelector(".md-header__inner");
  if (header && !header.querySelector(".md-header__discord")) {
    header.appendChild(discordLink("md-header__discord",
      '💬 <span class="md-header__discord-label">Get help on Discord</span>'));
  }

  const toc = document.querySelector(".md-sidebar--secondary .md-sidebar__inner");
  if (toc && !toc.querySelector(".md-toc__discord")) {
    const box = document.createElement("div");
    box.className = "md-toc__discord";
    box.innerHTML = "<strong>💬 Need help?</strong>" +
      "<p>Stuck on a lesson or want to learn Data Engineering with others? Join our Discord group.</p>";
    box.appendChild(discordLink("md-toc__discord-btn", "Join the Discord"));
    toc.appendChild(box);
  }
});
