#!/usr/bin/env python
"""
把 Markdown 教程转成单文件 HTML（带代码高亮、侧边导航、深色主题）
"""
import html
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
SRC = os.path.join(ROOT, "docs", "pyflink-tutorial.md")
DST = os.path.join(ROOT, "docs", "pyflink-tutorial.html")

CSS = """
:root{
  --bg:#0f1115;--bg2:#161922;--bg3:#1d212c;--bd:#2a2f3d;
  --tx:#e4e7ee;--tx2:#9aa3b5;--tx3:#6b7385;
  --acc:#7f77dd;--acc2:#9fe1cb;--warn:#ef9f27;--ok:#97c459;
  --code-bg:#12151c;--side-w:260px;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--tx);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;
  font-size:15px;line-height:1.75;-webkit-font-smoothing:antialiased}
#side{position:fixed;top:0;left:0;bottom:0;width:var(--side-w);background:var(--bg2);border-right:1px solid var(--bd);
  overflow-y:auto;padding:24px 0 40px}
#side h1{font-size:15px;font-weight:500;padding:0 20px 4px;color:var(--tx)}
#side .sub{font-size:12px;color:var(--tx3);padding:0 20px 20px;border-bottom:1px solid var(--bd);margin-bottom:12px}
#side a{display:block;padding:7px 20px;color:var(--tx2);text-decoration:none;font-size:13px;
  border-left:2px solid transparent;transition:.15s}
#side a:hover{color:var(--tx);background:var(--bg3)}
#side a.lv2{padding-left:34px;font-size:12.5px;color:var(--tx3)}
#side a.active{color:var(--acc2);border-left-color:var(--acc);background:rgba(127,119,221,.08)}
#main{margin-left:var(--side-w);padding:48px 56px 120px;max-width:1000px}
h1{font-size:28px;font-weight:500;margin:0 0 8px;letter-spacing:-.3px}
h2{font-size:21px;font-weight:500;margin:56px 0 16px;padding-bottom:10px;border-bottom:1px solid var(--bd);scroll-margin-top:20px}
h3{font-size:16.5px;font-weight:500;margin:32px 0 12px;color:var(--acc2);scroll-margin-top:20px}
h4{font-size:14.5px;font-weight:500;margin:22px 0 10px;color:var(--tx)}
p{margin:12px 0;color:#cdd3e0}
blockquote{margin:16px 0;padding:12px 18px;background:var(--bg2);border-left:3px solid var(--acc);
  border-radius:0 8px 8px 0;color:var(--tx2);font-size:14px}
blockquote p{margin:4px 0;color:var(--tx2)}
ul,ol{margin:12px 0 12px 24px}
li{margin:6px 0;color:#cdd3e0}
li>ul,li>ol{margin:6px 0 6px 20px}
code{font-family:"SF Mono",Consolas,"Cascadia Code",Menlo,monospace;font-size:13px;
  background:var(--bg3);padding:2px 6px;border-radius:4px;color:#e6b673}
pre{background:var(--code-bg);border:1px solid var(--bd);border-radius:10px;padding:16px 18px;
  overflow-x:auto;margin:16px 0;position:relative}
pre code{background:none;padding:0;color:#c9d1d9;font-size:12.8px;line-height:1.65}
.code-block{border:1px solid var(--bd);border-radius:10px;background:var(--code-bg);
  margin:16px 0;overflow:hidden}
.code-bar{display:flex;justify-content:space-between;align-items:center;
  padding:5px 12px;background:var(--bg2);border-bottom:1px solid var(--bd)}
.lang-tag{font-size:10.5px;color:var(--tx3);
  text-transform:uppercase;letter-spacing:.6px;font-family:monospace}
.copy-btn{font-size:11px;color:var(--tx2);background:var(--bg3);border:1px solid var(--bd);
  border-radius:6px;padding:2px 10px;cursor:pointer;font-family:inherit;line-height:1.6;
  transition:.15s;opacity:.6;white-space:nowrap}
.copy-btn:hover{opacity:1;color:var(--acc2);border-color:var(--acc)}
.copy-btn.done{opacity:1;color:var(--ok);border-color:var(--ok)}
.code-block pre{margin:0;border:none;border-radius:0;background:transparent}
table{width:100%;border-collapse:collapse;margin:18px 0;font-size:13.5px;
  border:1px solid var(--bd);border-radius:10px;overflow:hidden}
th{background:var(--bg3);text-align:left;padding:10px 14px;font-weight:500;color:var(--tx);
  border-bottom:1px solid var(--bd);font-size:13px}
td{padding:9px 14px;border-bottom:1px solid rgba(42,47,61,.6);color:#c3cad8;vertical-align:top}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(29,33,44,.5)}
hr{border:none;border-top:1px solid var(--bd);margin:40px 0}
a{color:var(--acc2)}
strong{font-weight:500;color:var(--tx)}
.hero{background:linear-gradient(135deg,rgba(127,119,221,.14),rgba(29,158,117,.09));
  border:1px solid var(--bd);border-radius:14px;padding:26px 28px;margin-bottom:32px}
.hero .tag{display:inline-block;font-size:11.5px;color:var(--acc2);background:rgba(29,158,117,.14);
  padding:3px 10px;border-radius:20px;margin-bottom:12px;letter-spacing:.3px}
.hero h1{margin-bottom:10px}
.hero p{color:var(--tx2);font-size:14px;margin:6px 0}
.hero .meta{display:flex;flex-wrap:wrap;gap:20px;margin-top:16px;font-size:12.5px;color:var(--tx3)}
.hero .meta b{color:var(--tx2);font-weight:500}
.lead{margin-top:0}
.kw{color:#ff7b72}.st{color:#a5d6ff}.cm{color:#8b949e;font-style:italic}
.nm{color:#79c0ff}.fn{color:#d2a8ff}.dc{color:#e6b673}
::-webkit-scrollbar{width:9px;height:9px}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:#333a4a;border-radius:5px}
::-webkit-scrollbar-thumb:hover{background:#414a5e}
@media(max-width:1100px){#side{display:none}#main{margin-left:0;padding:32px 24px 80px}}
"""


def highlight(code: str, lang: str) -> str:
    """
    轻量代码高亮。

    实现要点：先把字符串和注释「挖出来」用占位符替换，做关键字着色后再填回去。
    否则字符串里的 # 会被当成注释、关键字会被误着色。
    """
    esc = html.escape(code)
    stash = []

    def stash_it(m, cls):
        stash.append((cls, m.group(0)))
        return f"\x00{len(stash) - 1}\x00"

    if lang in ("python", "py"):
        # ① 先挖三引号字符串（可跨行）
        esc = re.sub(
            r"(?:&quot;&quot;&quot;[\s\S]*?&quot;&quot;&quot;|&#39;&#39;&#39;[\s\S]*?&#39;&#39;&#39;)",
            lambda m: stash_it(m, "st"), esc,
        )
        # ② 再挖普通字符串
        esc = re.sub(
            r"(?:&quot;(?:[^&\\]|\\.)*?&quot;|&#39;(?:[^&\\]|\\.)*?&#39;)",
            lambda m: stash_it(m, "st"), esc,
        )
        # ③ 再挖注释（此时字符串已被保护，不会误伤）
        esc = re.sub(r"(#[^\n]*)", lambda m: stash_it(m, "cm"), esc)
        # ④ 关键字
        # ⚠️ 必须【一次性】用单个正则替换全部关键字，不能 for 循环逐个替换！
        #   原因：逐个替换时，轮到 "class" 会把【上一轮自己插入的】
        #   <span class="kw"> 属性里的 "class" 又匹配上，标签被撕碎成：
        #     <span <span class="kw">class</span>="kw">from</span>
        #   页面上就直接显示成 `class="kw">from`。
        #   合并成一个正则后，每段文本只被扫描一次，不会二次替换。
        _kws = ("from|import|def|class|return|yield|if|elif|else|for|while|in|not|and|or|"
                "is|None|True|False|with|as|try|except|finally|raise|lambda|pass|global|"
                "self|async|await")
        esc = re.sub(
            rf"\b(?:{_kws})\b",
            lambda m: f'<span class="kw">{m.group(0)}</span>',
            esc,
        )
        # ⑤ 内置/常用名
        # 同样一次性替换，理由同上（这些名字若出现在标签属性里也会被误伤）
        _fns = ("print|len|list|dict|str|int|float|bool|sorted|range|enumerate|zip|"
                "json|re|os|sys|time")
        esc = re.sub(
            rf"\b(?:{_fns})\b(?=\()",
            lambda m: f'<span class="fn">{m.group(0)}</span>',
            esc,
        )

    elif lang == "sql":
        esc = re.sub(r"(&#39;(?:[^&]|&(?!39;))*?&#39;)", lambda m: stash_it(m, "st"), esc)
        esc = re.sub(r"(--[^\n]*)", lambda m: stash_it(m, "cm"), esc)
        # ⚠️ 一次性替换（同 Python 分支的理由，避免二次替换撕碎自己的标签）
        #    并用 m.group(0) 保留原文大小写，不要强制转大写
        _sql_kws = ("SELECT|FROM|WHERE|GROUP|BY|ORDER|LIMIT|JOIN|LEFT|RIGHT|INNER|OUTER|ON|"
                    "INSERT|INTO|CREATE|TABLE|WITH|AS|AND|OR|NOT|NULL|IS|COUNT|SUM|AVG|MAX|"
                    "MIN|ASC|DESC|LATERAL|WATERMARK|FOR|PRIMARY|KEY|END|CAST|OVER|PARTITION|"
                    "UNION|ALL|BETWEEN|INTERVAL|SECOND|MINUTE|HOUR|DAY|EXISTS|VALUES|"
                    "DISTINCT|USING|ENFORCED")
        esc = re.sub(
            rf"\b(?:{_sql_kws})\b",
            lambda m: f'<span class="kw">{m.group(0)}</span>',
            esc, flags=re.I,
        )

    elif lang in ("bash", "sh"):
        esc = re.sub(r"(&quot;[^&\n]*?&quot;|&#39;[^&\n]*?&#39;)", lambda m: stash_it(m, "st"), esc)
        esc = re.sub(r"(#[^\n]*)", lambda m: stash_it(m, "cm"), esc)
        # ⚠️ 一次性替换（同上）。(?<![\w.-]) 保证 docker-compose 里的 compose 不被高亮
        _sh_kws = ("cd|echo|export|source|bash|python|pip|uv|docker|compose|ls|mkdir|rm|set|"
                   "if|then|fi|for|do|done")
        esc = re.sub(
            rf"(?<![\w.-])\b(?:{_sh_kws})\b",
            lambda m: f'<span class="kw">{m.group(0)}</span>',
            esc,
        )

    elif lang in ("yaml", "yml"):
        esc = re.sub(r"(&quot;[^&\n]*?&quot;|&#39;[^&\n]*?&#39;)", lambda m: stash_it(m, "st"), esc)
        esc = re.sub(r"(#[^\n]*)", lambda m: stash_it(m, "cm"), esc)
        esc = re.sub(r"^(\s*)([\w.-]+)(:)", r'\1<span class="nm">\2</span>\3', esc, flags=re.M)

    elif lang == "json":
        esc = re.sub(r"(&quot;[^&\n]*?&quot;)", lambda m: stash_it(m, "st"), esc)

    # 填回被保护的内容
    # ⚠️ 必须【循环】替换直到没有占位符，不能只 re.sub 一次！
    #   原因：注释是在字符串【之后】挖的，所以注释内容里可能已经嵌着
    #   字符串的占位符。例如：
    #       # 报错：Encountered "f" at line N
    #   先挖字符串 "f" → \x002\x00，再挖注释时整行（含 \x002\x00）被存进 stash。
    #   最后 re.sub 是【单次扫描】，替换注释占位符时产生的文本里那个
    #   \x002\x00 不会再被处理，就直接泄漏到页面上，显示成乱码。
    def unstash(m):
        cls, raw = stash[int(m.group(1))]
        return f'<span class="{cls}">{raw}</span>'

    # 循环次数上限取 stash 长度 + 2，保证嵌套有多深都能还原干净
    for _ in range(len(stash) + 2):
        new = re.sub(r"\x00(\d+)\x00", unstash, esc)
        if new == esc:
            break
        esc = new
    return esc


def md_table_to_html(lines):
    """把 markdown 表格转 html"""
    rows = []
    for ln in lines:
        if re.match(r"^\s*\|[\s\-:|]+\|\s*$", ln):
            continue
        cells = [c.strip() for c in ln.strip().strip("|").split("|")]
        rows.append(cells)
    if not rows:
        return ""
    out = ["<table>"]
    out.append("<thead><tr>" + "".join(f"<th>{inline(c)}</th>" for c in rows[0]) + "</tr></thead>")
    out.append("<tbody>")
    for r in rows[1:]:
        out.append("<tr>" + "".join(f"<td>{inline(c)}</td>" for c in r) + "</tr>")
    out.append("</tbody></table>")
    return "\n".join(out)


def inline(text: str) -> str:
    """处理行内 markdown"""
    t = html.escape(text)
    t = re.sub(r"`([^`]+)`", r"<code>\1</code>", t)
    t = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", t)
    t = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<em>\1</em>", t)
    t = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', t)
    t = re.sub(r"<a name=\"[^\"]*\"></a>", "", t)
    return t


def convert(md: str):
    lines = md.split("\n")
    out, toc = [], []
    i = 0
    in_code = False
    code_buf, code_lang = [], ""
    in_quote = False
    quote_buf = []
    table_buf = []

    def flush_table():
        nonlocal table_buf
        if table_buf:
            out.append(md_table_to_html(table_buf))
            table_buf = []

    def flush_quote():
        nonlocal quote_buf
        if quote_buf:
            body = " ".join(quote_buf)
            out.append(f"<blockquote>{inline(body)}</blockquote>")
            quote_buf = []

    while i < len(lines):
        ln = lines[i]

        # 代码块
        if ln.strip().startswith("```"):
            if not in_code:
                flush_table(); flush_quote()
                in_code = True
                code_lang = ln.strip()[3:].strip() or "text"
                code_buf = []
            else:
                in_code = False
                hl = highlight("\n".join(code_buf), code_lang)
                # 代码块外面套一层 wrapper，顶栏放语言标签 + 复制按钮。
                # 不用绝对定位把按钮浮在代码上 —— 那样长行横向滚动时会盖住内容。
                out.append(
                    '<div class="code-block">'
                    '<div class="code-bar">'
                    f'<span class="lang-tag">{code_lang}</span>'
                    '<button class="copy-btn" type="button" '
                    'onclick="copyCode(this)">复制</button>'
                    '</div>'
                    f'<pre><code>{hl}</code></pre>'
                    '</div>'
                )
                code_buf = []
            i += 1
            continue
        if in_code:
            code_buf.append(ln)
            i += 1
            continue

        # 表格
        if ln.strip().startswith("|"):
            flush_quote()
            table_buf.append(ln)
            i += 1
            continue
        else:
            flush_table()

        # 引用
        if ln.strip().startswith(">"):
            q = ln.strip().lstrip(">").strip()
            quote_buf.append(q)
            i += 1
            continue
        else:
            flush_quote()

        # 标题
        m = re.match(r"^(#{1,4})\s+(.*)$", ln)
        if m:
            lvl, txt = len(m.group(1)), m.group(2)
            if lvl == 1:
                out.append(f'<h1>{inline(txt)}</h1>')
            elif lvl == 2:
                aid = f"h2-{len(toc)}"
                toc.append((txt, aid, 1))
                out.append(f'<h2 id="{aid}">{inline(txt)}</h2>')
            elif lvl == 3:
                aid = f"h3-{len(toc)}"
                toc.append((txt, aid, 2))
                out.append(f'<h3 id="{aid}">{inline(txt)}</h3>')
            else:
                out.append(f"<h4>{inline(txt)}</h4>")
            i += 1
            continue

        # 分隔线
        if re.match(r"^\s*---+\s*$", ln):
            out.append("<hr>")
            i += 1
            continue

        # 列表
        if re.match(r"^\s*[-*]\s+", ln):
            items = []
            while i < len(lines) and re.match(r"^\s*[-*]\s+", lines[i]):
                items.append(re.sub(r"^\s*[-*]\s+", "", lines[i]))
                i += 1
            out.append("<ul>" + "".join(f"<li>{inline(x)}</li>" for x in items) + "</ul>")
            continue
        if re.match(r"^\s*\d+\.\s+", ln):
            items = []
            while i < len(lines) and re.match(r"^\s*\d+\.\s+", lines[i]):
                items.append(re.sub(r"^\s*\d+\.\s+", "", lines[i]))
                i += 1
            out.append("<ol>" + "".join(f"<li>{inline(x)}</li>" for x in items) + "</ol>")
            continue

        # 空行
        if not ln.strip():
            i += 1
            continue

        # 段落
        out.append(f"<p>{inline(ln)}</p>")
        i += 1

    if in_code and code_buf:
        hl = highlight("\n".join(code_buf), code_lang)
        out.append(f'<pre><code>{hl}</code></pre>')
    flush_table(); flush_quote()
    return out, toc


def build(src=None, dst=None, meta=None):
    src = src or SRC
    dst = dst or DST
    meta = meta or {}

    with open(src, "r", encoding="utf-8") as f:
        md = f.read()

    body, toc = convert(md)

    nav_title = meta.get("nav_title", "PyFlink 完整教程")
    nav_sub = meta.get("nav_sub", "8 个可运行示例 · Flink 1.20")
    page_title = meta.get("page_title", "PyFlink 从入门到生产 · 完整学习教程")
    hero_tag = meta.get("hero_tag", "Flink 1.20.0 · Python 3.11 · 可运行")
    hero_h1 = meta.get("hero_h1", "PyFlink 从入门到生产")
    hero_lead = meta.get("hero_lead",
        "一套可以真正跑起来的 PyFlink 教程。从 WordCount 到实时风控系统，"
        "8 个递进示例，每个都配了完整注释和踩坑说明。")
    hero_meta = meta.get("hero_meta", [
        ("8", "个可运行示例"),
        ("12", "章完整讲解"),
        ("零依赖", "即可试跑"),
        ("Docker", "一键环境"),
    ])

    nav = [f'<a href="#top" style="font-weight:500;color:var(--tx)">{html.escape(nav_title)}</a>']
    for txt, aid, lvl in toc:
        cls = "lv2" if lvl == 2 else ""
        nav.append(f'<a href="#{aid}" class="{cls}">{html.escape(txt)}</a>')

    meta_html = "".join(f"<span><b>{html.escape(str(a))}</b> {html.escape(b)}</span>"
                        for a, b in hero_meta)

    page = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(page_title)}</title>
<style>{CSS}</style>
</head>
<body id="top">
<nav id="side">
  <h1>{html.escape(nav_title)}</h1>
  <div class="sub">{html.escape(nav_sub)}</div>
  {''.join(nav)}
</nav>
<main id="main">
<div class="hero">
  <span class="tag">{html.escape(hero_tag)}</span>
  <h1>{html.escape(hero_h1)}</h1>
  <p class="lead">{html.escape(hero_lead)}</p>
  <div class="meta">
    {meta_html}
  </div>
</div>
{''.join(body)}
</main>
<script>
const links=[...document.querySelectorAll('#side a')];
const targets=links.map(a=>document.querySelector(a.getAttribute('href'))).filter(Boolean);
function onScroll(){{
  let cur=0;
  targets.forEach((t,i)=>{{ if(t.getBoundingClientRect().top<120) cur=i; }});
  links.forEach((a,i)=>a.classList.toggle('active',i===cur));
}}
document.addEventListener('scroll',onScroll,{{passive:true}});
onScroll();

/* ---- 代码块一键复制 ---- */
function copyCode(btn){{
  var box=btn.closest('.code-block');
  var code=box?box.querySelector('pre code'):null;
  /* 用 textContent 而不是 innerText：不受 CSS 影响，
     且 <code> 里不含语言标签，复制出来的就是纯代码 */
  var text=code?code.textContent:'';
  var done=function(){{
    btn.textContent='已复制';btn.classList.add('done');
    setTimeout(function(){{btn.textContent='复制';btn.classList.remove('done');}},1500);
  }};
  if(navigator.clipboard&&window.isSecureContext){{
    navigator.clipboard.writeText(text).then(done).catch(function(){{fallbackCopy(text,done);}});
  }}else{{
    /* file:// 打开或 http 环境下 clipboard API 不可用，走兜底 */
    fallbackCopy(text,done);
  }}
}}
function fallbackCopy(text,done){{
  var ta=document.createElement('textarea');
  ta.value=text;ta.setAttribute('readonly','');
  ta.style.position='fixed';ta.style.top='-9999px';ta.style.opacity='0';
  document.body.appendChild(ta);
  ta.select();ta.setSelectionRange(0,text.length);
  var ok=false;
  try{{ok=document.execCommand('copy');}}catch(e){{ok=false;}}
  document.body.removeChild(ta);
  if(ok){{done();}}else{{alert('复制失败，请手动选中代码复制');}}
}}
</script>
</body>
</html>"""

    with open(dst, "w", encoding="utf-8") as f:
        f.write(page)
    print(f"已生成 {dst}")
    print(f"  大小：{os.path.getsize(dst) / 1024:.1f} KB")
    print(f"  导航项：{len(toc)} 个")


if __name__ == "__main__":
    import sys

    # 用法:
    #   python build_html.py                                    # 默认（PyFlink 教程）
    #   python build_html.py <源md> [目标html]                   # 按文件名自动选标题
    #   python build_html.py <源md> [目标html] --title "页标题" --nav "导航标题" --sub "副标题"
    argv = sys.argv[1:]

    def _opt(name, default=None):
        """取 --name value 形式的选项"""
        if name in argv:
            i = argv.index(name)
            if i + 1 < len(argv):
                return argv[i + 1]
        return default

    # 过滤掉 --xxx value 这类选项，剩下的就是位置参数
    positional = []
    skip = False
    for i, a in enumerate(argv):
        if skip:
            skip = False
            continue
        if a in ("--title", "--nav", "--sub", "--tag", "--h1", "--lead"):
            skip = True
            continue
        positional.append(a)

    if positional:
        _src = positional[0]
        _dst = positional[1] if len(positional) > 1 else _src.rsplit(".", 1)[0] + ".html"
        _base = os.path.basename(_src).lower()

        _opt_title = _opt("--title")
        _opt_nav = _opt("--nav")
        _opt_sub = _opt("--sub")
        _opt_tag = _opt("--tag")
        _opt_h1 = _opt("--h1")
        _opt_lead = _opt("--lead")

        if _opt_title or _opt_nav:
            # 命令行显式指定了标题，直接用它
            build(_src, _dst, {
                "nav_title": _opt_nav or _opt_title or "文档",
                "nav_sub": _opt_sub or "",
                "page_title": _opt_title or _opt_nav,
                "hero_tag": _opt_tag or "学习文档",
                "hero_h1": _opt_h1 or _opt_nav or _opt_title,
                "hero_lead": _opt_lead or "",
            })
        elif "docker" in _base:
            build(_src, _dst, {
                "nav_title": "Docker 部署教程",
                "nav_sub": "pg-course · pyflink-tutorial",
                "page_title": "Docker 部署详细教程 · PostgreSQL 主从 + PyFlink 集群",
                "hero_tag": "Docker Desktop 4.89 · WSL2 · 实测",
                "hero_h1": "Docker 部署详细教程",
                "hero_lead": "两套部署的完整手册：PostgreSQL 16 主从复制，以及 PyFlink 集群。"
                             "含数据盘迁移、目录位置、启动运行、故障排查。",
                "hero_meta": [
                    ("2", "套部署环境"),
                    ("6", "章完整讲解"),
                    ("全路径", "标注存储位置"),
                    ("实测", "踩坑记录"),
                ],
            })
        elif "dbeaver" in _base:
            build(_src, _dst, {
                "nav_title": "DBeaver 操作手册",
                "nav_sub": "连接 pg-course",
                "page_title": "DBeaver 连接 pg-course 操作手册",
                "hero_tag": "Windows · DBeaver · PostgreSQL 16 主从",
                "hero_h1": "DBeaver 连接 pg-course",
                "hero_lead": "图形界面接入指南：连接参数、账号权限体系、主从复制验证、"
                             "实用功能与故障排查。",
                "hero_meta": [
                    ("2", "个连接（主/从）"),
                    ("8", "节完整讲解"),
                    ("134", "个 SQL 示例"),
                    ("全实测", "可直接跑"),
                ],
            })
        else:
            build(_src, _dst)
    else:
        build()
