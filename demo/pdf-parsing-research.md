# Modern PDF Parsing: A Deep-Dive Reference for Practitioners Building Agentic Search & Retrieval Systems

*Compiled May 2026 — covers research and tooling through early 2026*

---

## Table of Contents

1. [The PDF Format at the Binary Level](#1-the-pdf-format-at-the-binary-level)
2. [Core Parsing Challenges](#2-core-parsing-challenges)
3. [Traditional Parsing Approaches](#3-traditional-parsing-approaches)
4. [Modern ML-Based Approaches](#4-modern-ml-based-approaches)
5. [Approaches for RAG and Agentic Retrieval](#5-approaches-for-rag-and-agentic-retrieval)
6. [The Tools Landscape](#6-the-tools-landscape)
7. [Current State of the Art and Open Problems](#7-current-state-of-the-art-and-open-problems)
8. [Recommendations by Use Case](#8-recommendations-by-use-case)

---

## 1. The PDF Format at the Binary Level

### 1.1 What PDF Actually Is

PDF (Portable Document Format), standardized as ISO 32000, is fundamentally a *device-independent page-description language* descended from PostScript. Its design goal — to render a document identically on any device — is precisely what makes it a nightmare for text extraction. The spec prioritizes visual fidelity over semantic structure.

A PDF file is a collection of binary objects organized into a four-part structure:

```
%PDF-1.x         ← Header (magic bytes + version)
... objects ...  ← Body (the actual content)
xref table       ← Cross-reference index (byte offsets)
trailer          ← Points to xref, root object, and encryption dict
%%EOF
```

**Objects.** Everything in a PDF is an object. There are eight primitive types:
- `Boolean` — `true` / `false`
- `Integer` / `Real` — numbers
- `String` — byte strings in `(...)` literal form or `<...>` hex form
- `Name` — `/FontName`, `/Type`
- `Array` — `[obj1 obj2 ...]`
- `Dictionary` — `<< /Key value ... >>`
- `Stream` — a dictionary followed by a raw byte payload delimited by `stream` / `endstream`
- `Null`

Each indirect object is addressed as `<objnum> <gennum> obj ... endobj`. A full document is basically a graph of these objects.

**Cross-Reference (XRef) Tables.** The xref section maps object numbers to byte offsets, enabling random access. PDF 1.4 and earlier use a text-based xref table. PDF 1.5+ introduced *compressed xref streams* — the same offset table stored as a binary stream object (optionally with Flate/zlib compression), which significantly reduces file size but requires a stream parser before you can do anything else.

Incremental updates add new xref sections at the end of the file, with each new trailer pointing to the previous one. This means a file can contain multiple xref sections forming a linked chain; understanding all of them is essential for correct object resolution.

**Content Streams.** Each page has one or more *content streams* — sequences of PDF operators that describe drawing operations. The instruction set is stack-based (like PostScript). Key operators include:
- `Tf` — select font and size
- `Tm` / `Td` / `TD` / `T*` — set text matrix / move text position
- `Tj` / `TJ` — show text (single string / array of strings with inter-character spacing adjustments)
- `cm` — concatenate matrix to current transformation matrix
- `BT` / `ET` — begin/end text block
- `q` / `Q` — push/pop graphics state
- `Do` — invoke an XObject (embedded image or Form XObject)

Text in a content stream is *not* stored in reading order. The PDF renderer is free to emit drawing commands in any sequence — a single sentence may be composed of dozens of `Tj` calls interleaved with repositioning operators, kerning adjustments, and font switches.

**Fonts and CMaps.** This is the most treacherous part for text extraction. Fonts in PDF are represented as dictionaries with several sub-structures:

- **Encoding** — maps character codes (1-byte) to glyph names or Unicode values. Simple fonts (Type 1, TrueType, Type 3) may use one of three built-in encodings (MacRomanEncoding, WinAnsiEncoding, PDFDocEncoding) or a custom Differences array.
- **CIDFont (Type 0 / CID composite fonts)** — used for CJK and Unicode-range text. Character codes are 2+ bytes. The mapping chain is: character code → CID → glyph ID, with an additional `/CIDSystemInfo` structure.
- **ToUnicode CMap** — an optional stream embedded in the font dict that maps character codes to Unicode code points via `beginbfchar` / `beginbfrange` sections. Without a ToUnicode map, text extraction must rely on glyph-name-to-Unicode heuristics, which frequently fail.
- **Font subsetting** — PDF producers typically embed only the glyphs actually used, stripping the rest of the font program. The glyph IDs in the subset do not correspond to standard encoding tables. This means text extraction tools must parse the embedded program (CFF, TrueType/OpenType tables) plus the ToUnicode map rather than using any standard lookup.

**Streams and Filters.** Raw stream data can be compressed or encoded with filters chained together: `/FlateDecode` (zlib), `/LZWDecode`, `/CCITTFaxDecode` (fax for images), `/DCTDecode` (JPEG), `/JPXDecode` (JPEG 2000), `/ASCII85Decode`, `/ASCIIHexDecode`. Content streams are routinely Flate-compressed; images use DCT/JPX. A robust parser must handle filter chains in the correct order.

### 1.2 Key Spec Versions

| Version | Key Changes |
|---------|------------|
| PDF 1.0 (1993) | Initial Acrobat release |
| PDF 1.2 | Interactive features (forms, links) |
| PDF 1.3 | Digital signatures, RC4-40 encryption |
| PDF 1.4 | Transparency model, RC4-128 |
| PDF 1.5 | Compressed xref streams, object streams, AES-128 |
| PDF 1.6 | OpenType fonts, AES-128 encryption |
| PDF 1.7 / ISO 32000-1 (2008) | Full ISO standardization |
| PDF 2.0 / ISO 32000-2 (2017) | AES-256, improved tagged PDF, deprecated RC4 |
| PDF/A series | Archival variants (PDF/A-1, -2, -3, -4); require embedded fonts and XMP metadata |
| PDF/UA (Universal Accessibility) | Requires tagged PDF with semantic structure tree |

PDF 2.0 is relatively rare in the wild (2026). The vast majority of documents are PDF 1.4–1.7. Tagged PDF (with a `/StructTree`) is present in accessibility-compliant documents and can make extraction significantly easier — but most real-world PDFs are untagged.

### 1.3 Why PDF is Hard to Parse (By Design)

The core tension: PDF was designed so that a renderer needs only to *paint pixels*, not understand semantic structure. Consequences:
- Text runs can be in arbitrary visual order; reading order is not encoded.
- A single word may be decomposed across multiple `Tj` calls, font changes, or even separate pages.
- There is no standard text segmentation; "paragraphs" do not exist as first-class objects.
- Tables are pure geometry — rows and columns are inferred from bounding box alignment.
- Ligatures (ﬁ, ﬂ) are single glyphs with no semantic decomposition.
- Rotated text, text in Form XObjects, and text on non-rectangular paths are all valid.

---

## 2. Core Parsing Challenges

### 2.1 Text Extraction: Encoding Hell

**The ToUnicode problem.** When a font has a correct ToUnicode CMap, extraction is straightforward. When it doesn't — which happens routinely with older documents, certain CJK fonts, and heavily subsetted fonts — you must fall back to:
1. Glyph name lookup in the Adobe Glyph List.
2. Unicode value embedded in the glyph name (e.g., `/uni0041` → 'A').
3. Platform-specific cmap tables in the embedded font binary.
4. Heuristic encoding guessing.

Known failure modes, documented in open issues as of 2024-2025:
- `pdfminer.six`: completely incorrect ToUnicode CMap handling for non-identity CMaps, producing garbled CJK extraction.
- `pypdf`: incorrect handling of Type 0 CID fonts where ToUnicode maps directly from byte sequences rather than through CID intermediaries.
- `PyMuPDF`: generally more robust, but still fails on certain subsetted CJK fonts with incorrect or missing ToUnicode.

**Font subsetting.** A subset font only contains the glyphs used in the document. The glyph ordering is arbitrary — glyph 0 might be 'A', glyph 1 might be 'Z', glyph 2 might be 'B'. Without a ToUnicode map (or a working encoding), extraction produces garbage. This is especially common in PDFs generated by LaTeX/pdflatex with subsetted Type 1 fonts.

**Ligatures and special glyphs.** A ligature like `ﬁ` (fi) or `ﬄ` (ffl) is a single glyph. If the ToUnicode map correctly maps it to `U+FB01`, extraction is fine. If not, extractors often produce blank space or garbage. Mathematical bold/italic alphabets, OpenType alternate characters, and decorative glyphs suffer similarly.

**Vertical text and RTL.** Japanese/Chinese vertical writing, Arabic/Hebrew right-to-left text, and bidirectional (bidi) text all require special handling. Character order in the content stream may be logical (semantic) or visual (display) depending on the authoring tool. Correct extraction requires Unicode Bidi Algorithm (UBA) application, which few tools implement fully.

### 2.2 Layout Reconstruction

**Reading order.** PDF has no explicit reading order. A two-column academic paper may have its content streams interspersed: some tokens from the left column, some from the right, some from the header, in the order the PDF producer emitted them. Reconstructing reading order requires spatial reasoning: group text into blocks, identify columns, order blocks top-to-bottom left-to-right (or culturally appropriate order).

Classic approach: Docstrum (bottom-up, uses nearest-neighbor analysis) or RLSA (recursive X-Y cut) for column detection. These work acceptably for simple single-column or dual-column layouts but fail on:
- Mixed-layout pages (e.g., one column at top, two columns below).
- Text in sidebars, callout boxes, or overlapping regions.
- Tables of contents where leader dots and page numbers interrupt the flow.
- Multi-page articles where columns span page boundaries.

**Tables.** Tables are the hardest structural element to extract. PDF has no native "table" object. A table is usually rendered as:
- Individual `Tj` text commands positioned with sub-point precision.
- Lines drawn with `l` / `re` operators (or invisible guides).
- Or no lines at all (whitespace-separated columns).

The two main heuristic approaches are:
- **Line-based**: detect visible ruling lines using graphics operators, use them to define cells. Works for bordered tables, fails for borderless.
- **Whitespace analysis**: project text into rows and columns based on x/y alignment and gaps. Works for simple tables, breaks on merged cells, straddle cells, or ragged column widths.

**Headers, footers, and page numbers.** These must be identified and either stripped or tagged separately to avoid contaminating running text. Heuristic approaches check for repeating patterns at page top/bottom, consistent y-coordinates across pages, and short text length. Fails when headers contain variable content (chapter names) or when footer text wraps to multiple lines.

**Figures and captions.** Figures are typically embedded as image XObjects with no textual content. Captions are nearby text blocks. Correctly associating a caption with its figure requires proximity analysis and heuristics about label patterns ("Figure 3:", "Fig. 3."). This association is often lost entirely in plain text extraction.

**Footnotes.** Inline footnote markers are usually superscript characters within the text stream. The footnote body is typically at the bottom of the page, sometimes separated by a ruling line. Linking the marker to its body, and deciding whether to inline or append the content, is non-trivial. Cross-page footnotes (where the body continues on the next page) are essentially never handled correctly by traditional tools.

### 2.3 Scanned vs. Born-Digital PDFs

**Born-digital PDFs** contain actual text objects in the content stream. For well-formed documents with correct font encoding, extraction quality approaches 100% character accuracy.

**Scanned PDFs** are images embedded in a PDF container. The content stream contains `Do` operators invoking image XObjects; there is no text. These require full OCR. Key challenges:
- Image quality: resolution below 200 DPI dramatically reduces OCR accuracy.
- Skew and rotation: pages scanned at an angle must be deskewed before OCR.
- Background noise, coffee stains, bleedthrough from the opposite side.
- Mixed documents: some pages scanned, some born-digital.

**Pseudo-born-digital (tagged scans).** A common failure mode: OCR was run at document creation time, and the resulting text layer was embedded invisibly behind the image. The text may be correct (if OCR was good) or completely wrong (common in older scans with poor OCR). Parsers that use the text layer without validation produce confident-but-wrong output. Detection: check if text coordinates align with actual image content, or if character confidence metrics are available.

### 2.4 Mathematical Formulas

Math in PDF is almost universally represented as a collection of individual character glyphs and graphics primitives. An integral sign is a large glyph. Subscripts and superscripts are text positioned with a vertical offset and font size change. The logical structure `∫₀¹ f(x)dx` is nowhere in the PDF — it must be reconstructed from spatial relationships between glyphs.

State-of-the-art formula extraction (as of 2025):
- Traditional tools: complete failure — output fragments, garbage, or empty strings.
- Mathpix: trained specifically on math, produces LaTeX output, highest accuracy for formulas.
- Nougat: trained on arXiv papers, understands math in context of academic text.
- GOT-OCR 2.0: supports LaTeX output for formulas.
- MinerU 2.5: formula parsing via embedded vision model.

A 2024 benchmark (arXiv:2512.09874) comparing parsers on mathematical formula extraction found that even the best tools struggle with compound symbols, detached root symbols, and ambiguous notation; no parser achieved perfect semantic equivalence on complex expressions.

### 2.5 Code Blocks

Source code in PDFs (common in textbooks, papers, and documentation) loses all semantic significance in extraction. Indentation is represented by spaces, which may or may not be preserved depending on the font (fixed-width vs. proportional). Syntax highlighting — color information embedded in the PDF — is discarded. Line breaks within a code block are indistinguishable from paragraph breaks. Few tools provide any special handling.

### 2.6 Multi-Language and RTL Text

Arabic and Hebrew run right-to-left; mixed bidi passages (e.g., English URL embedded in Hebrew text) require the Unicode Bidirectional Algorithm. Most PDF parsers do not apply UBA, producing reversed or scrambled output. CJK languages with vertical writing need 90-degree character rotation understanding. Devanagari and other Indic scripts use complex ligature rules. Thai and other scripts without word boundaries require language-specific segmentation.

### 2.7 Encrypted and DRM'd PDFs

PDF 1.3+ supports password-based encryption using RC4 (deprecated) or AES. There are two passwords: a user password (required to open) and an owner password (required to change permissions). Standard permission flags (copy, print, modify) are PDF-level and not enforced by all viewers — Firefox's PDF.js, for example, ignores them.

True DRM (as opposed to standard PDF encryption) uses third-party systems like Adobe LiveCycle DRM, FileOpen, or Locklizard. These are essentially unaddressable programmatically without the DRM client library.

For pipeline use: if a document has a user password, you need the password. If it's DRM-protected with a third-party system, you cannot parse it programmatically. Standard permissions-only documents (no user password) can typically be parsed by any library regardless of the copy-restriction flag.

### 2.8 Large PDFs (1000+ Pages)

Large PDFs introduce performance and memory challenges:
- Sequential cross-reference walking on malformed xref chains is O(pages²) in some implementations.
- Loading the entire document into memory is impractical; streaming parsers are necessary.
- Thread-safety: many PDF libraries are not safe for parallel page processing.
- PyMuPDF (MuPDF engine) handles large documents well due to its C core and lazy object loading.
- pdfminer.six struggles above ~500 pages due to Python-native parsing overhead.
- For LLM pipelines: processing 1000-page docs typically requires chunked batch processing with page-level parallelism.

---

## 3. Traditional Parsing Approaches

### 3.1 Text Extraction Pipelines

**pdfminer.six** (Python, actively maintained fork of pdfminer)
The original workhorse. Pure-Python parser that provides deep access to PDF internals: character-level bounding boxes, font attributes, character codes, and decoded Unicode. Key class: `PDFConverter` produces `LTPage` objects containing `LTTextBox`, `LTTextLine`, `LTChar`. Extremely flexible but slow (~5-10x slower than PyMuPDF), and ToUnicode handling has known correctness issues. Still valuable for debugging encoding problems because of its transparency.

**pypdf (formerly PyPDF2)**
Pure Python, lightweight, permissive license. Handles basic text extraction, metadata, merging, splitting, and encryption. Text extraction quality is significantly lower than pdfminer.six or PyMuPDF for complex documents; known ToUnicode CMap handling bugs remain open as of 2025. Best suited for simple document manipulation (merging, page extraction) rather than text extraction.

**PyMuPDF (fitz, import pymupdf)**
Python bindings for MuPDF, a fast C-based rendering engine. The most performant Python PDF library by a large margin — benchmarks show 10-50x speed improvements over pure-Python alternatives. Features: text extraction with bounding boxes, page rendering to PNG/SVG, TOC extraction, link extraction, page manipulation. The `pymupdf4llm` extension adds structured Markdown output optimized for LLM consumption. Maintained by Artifex (MuPDF creators). ~50 million monthly downloads as of 2025. The default choice for production born-digital PDF extraction.

*Performance data*: PyMuPDF and pypdfium2 consistently top text extraction benchmarks across general documents. The py-pdf/benchmarks repository shows PyMuPDF as 3-5x faster than pdfplumber and 8-15x faster than pdfminer.six on a common set of PDFs.

**pdfplumber** (Python)
Built on pdfminer.six, adds whitespace analysis for table extraction. Provides `page.extract_table()` and `page.extract_tables()` using configurable horizontal/vertical line detection and text-alignment heuristics. Best in class for rule-based table extraction from born-digital PDFs with clear whitespace separators. Includes a visual debugging tool (`page.to_image()`) that helps tune extraction parameters. Slower than PyMuPDF; recommended for documents where table quality matters more than speed.

**pypdfium2** (Python)
Python bindings for PDFium (Chromium's PDF engine, the same one in Chrome). Fast, accurate, and Apache-licensed. Second only to PyMuPDF in speed. Fewer high-level convenience functions but very reliable low-level extraction. Used as the backend for `pdftext` (see below).

**Apache PDFBox** (Java)
Mature, well-maintained Java library from the Apache Software Foundation. Full PDF specification support including PDF/A validation, digital signatures, form filling, and AcroForm handling. Text extraction via `PDFTextStripper`. Good CJK support. The reference implementation for many enterprise Java PDF workflows. Slower than C-based alternatives.

**iText / iText 7** (Java/C#)
Comprehensive commercial library (with AGPL open-source edition) supporting creation, editing, and extraction. iText 7's `PdfTextExtractor` uses location-based text extraction strategies. Strong PDF/A and PDF/UA compliance. Commonly used in enterprise document management.

**Ghostscript**
PostScript/PDF interpreter that can render pages to images or extract text via the `text` device (`-dSAFER -sDEVICE=txtwrite`). Text quality is often poor because Ghostscript optimizes for rendering fidelity rather than semantic text extraction. Useful as a last resort for unusual PDFs that other tools fail on.

### 3.2 OCR-Based Pipelines

**Tesseract** (open source, Apache 2.0)
The dominant open-source OCR engine, now at v5 with LSTM-based recognition. Supports 100+ languages. Clean scan accuracy: 89-94% on well-scanned documents at 300+ DPI; drops to 65-80% on degraded/noisy documents. Known weakness: multi-column layout detection is poor — columns are often mixed. `pytesseract` provides the Python interface; `OCRmyPDF` wraps it for PDF-specific workflows (adds deskew, cleans artifacts, embeds OCR layer).

**PaddleOCR** (open source, Apache 2.0, by Baidu)
A complete OCR pipeline including detection (DBNet), recognition (CRNN), and a table recognition module. Supports 80+ languages. Generally outperforms Tesseract on complex layouts due to its detection model. Table recognition is a notable strength. Actively maintained; v4 (2024) added improved detection and recognition models.

**ABBYY FineReader / ABBYY Cloud OCR SDK**
Commercial, widely considered the most accurate traditional OCR system. Layout analysis is particularly strong — identifies columns, tables, headers/footers with high fidelity. Reported accuracy: 96-98% with document-specific tuning, 85-92% out-of-box. Used extensively in legal, financial, and government document processing. Expensive; primarily relevant for enterprises with strict accuracy requirements.

**Adobe Acrobat OCR**
Uses Adobe's Sensei AI. Strong accuracy on typical business documents. Embeds a text layer in the PDF (creates a searchable PDF). Not programmatically accessible except through Adobe's commercial APIs.

### 3.3 Heuristic Layout Analysis

The classical pre-ML approach to layout analysis:

**X-Y Cut algorithm**: Recursively bisect a page into regions along horizontal and vertical gaps. Simple, fast, works well for newspaper-style strictly-gridded layouts. Fails catastrophically on arbitrary layouts, rotated content, or overlapping regions.

**Docstrum**: Bottom-up approach using nearest-neighbor analysis of character clusters. Forms text lines from character pairs at similar inter-character distances and angles, then groups lines into blocks. More robust than X-Y Cut for technical documents.

**Whitespace column detection** (pdfplumber): Project text x-coordinates onto a 1D histogram; identify gaps between columns. Works well for 2- or 3-column academic papers; breaks on mixed layouts or irregular column widths.

**RLSA (Run Length Smoothing Algorithm)**: Smear white space to connect nearby text elements, then identify connected regions. One of the oldest layout methods; still sometimes seen in preprocessing pipelines.

All heuristic methods share fundamental limitations: they are brittle to layout variations, require parameter tuning per document type, and have no understanding of semantic structure.

### 3.4 PDF-to-HTML/XML Conversion

Tools like `pdf2htmlEX`, PDFMiner's HTML output, and `pdftohtml` (poppler) convert PDFs to HTML that preserves visual layout via absolute positioning. This preserves spatial relationships but produces extraction-hostile HTML (span soup). Some XML approaches (PDFMiner's LAParams output, Docling's JSON) provide structured hierarchical output.

---

## 4. Modern ML-Based Approaches

### 4.1 Document Layout Analysis Models

The breakthrough: treat layout analysis as an object detection problem on the rendered page image. This sidesteps the font encoding problem entirely — the model works on pixels, not text characters.

**LayoutParser** (2021)
Early framework for deep-learning-based layout analysis using Detectron2 as a backend. Provided a unified API with pre-trained models for PubLayNet, Prima, and HJDataset. Influential in establishing the vision-based approach for academic document parsing. Now somewhat dated as a framework but the models remain useful.

**DiT (Document Image Transformer)** (Microsoft, 2022)
A self-supervised Vision Transformer (ViT) pre-trained on IIT-CDIP (42M document images). Used as a backbone for document understanding tasks. DiT-Cascade-L achieves state-of-the-art (pre-2024) layout detection on DocLayNet. The key insight: document images have distinct visual statistics from natural photos, and pre-training on document images substantially improves downstream task performance.

**DocLayout-YOLO** (OpenDataLab, Oct 2024 — arXiv:2410.12628)
The current speed-accuracy frontier for layout detection. Based on YOLOv10 with two key innovations:
1. **Mesh-candidate BestFit algorithm**: frames training data synthesis as a 2D bin-packing problem, generating the diverse DocSynth-300K dataset (300K synthetic pages).
2. **Global-to-Local Controllable Receptive Module (GL-CRM)**: handles multi-scale variation of document elements (a section title and a footnote can differ dramatically in size).

Benchmark results on DocStructBench (a new challenging benchmark introduced in the paper):
- Outperforms DiT-Cascade-L by significant margin in accuracy.
- **14.3× faster FPS than DiT-Cascade-L** (the best previous multimodal method).
- Comparable speed to plain YOLOv10 (unimodal) while outperforming it in accuracy.
- Achieves superior performance on 3 of 4 DocStructBench subsets.

DocLayout-YOLO is now the backbone detector in MinerU, Docling's newer model stack, and several production pipelines.

**PP-DocLayout** (Baidu PaddlePaddle, 2025)
A unified document layout detection model supporting 23 layout categories. Achieves 91.0 mAP on DocLayNet-P with notably improved detection of challenging elements like seals and signatures. Designed for high-speed large-scale data construction pipelines.

### 4.2 Vision-Language Models for Document Understanding

The new paradigm: treat the entire document page as an image and use a VLM to produce structured text output end-to-end. No separate OCR or layout detection step.

**Donut** (CLOVA AI / Naver, ECCV 2022)
First major OCR-free document understanding model. Architecture: Swin Transformer visual encoder + BART language decoder. Trained on SynthDoG (synthetic document generator). Produces sequences of tokens (JSON or text) directly from page images. No OCR engine in the loop. Achieves state-of-the-art on DocVQA, CORD (receipt understanding), and RVL-CDIP (document classification). Inference is relatively slow (several seconds per page on GPU).

**Nougat** (Meta AI, 2023 — arXiv:2308.13418; published at ICLR 2024)
Extends Donut architecture specifically for scientific/academic PDFs. Trained on pairs of arXiv PDFs and their LaTeX source. Produces Markdown output including proper LaTeX for equations, tables, and structured headings. A genuine breakthrough for scientific paper extraction.

Strengths:
- Handles mathematical notation correctly.
- Produces properly structured Markdown.
- Recovers readable text even from complex layouts.

Weaknesses:
- Trained primarily on arXiv; generalizes poorly to other domains (financial reports, legal documents, textbooks).
- "Hallucination" problem: the autoregressive decoder can generate text not present in the image (up to ~30% of output in some benchmarks on non-arXiv documents).
- Slow: ~5-20 seconds per page depending on GPU.
- Does not handle multi-column layouts especially well.

**GOT-OCR 2.0** (General OCR Theory, 2024)
An OCR-focused VLM trained to handle diverse OCR tasks in a single model: scene text, document text, math formulas (LaTeX output), music notation, molecular structures (SMILES), tables (HTML/LaTeX), and code. Key features:
- Whole-page and crop modes.
- Formatted output: markdown, LaTeX, SMILES.
- Supports sliced inference for high-resolution images.
- 580M parameter encoder-decoder (SigLIP + Qwen).
- Dramatically reduces the need for task-specific models.

**SmolDocling** (IBM Research + Hugging Face, March 2025)
A 256M-parameter VLM for complete document OCR. Introduces **DocTags**, a universal markup format that captures all page elements (charts, tables, forms, code, equations, footnotes, captions) plus their spatial and contextual relationships. Despite its small size:
- F1-score of 0.80 on DocLayNet full-page transcription.
- BLEU of 0.58 — outperforming Qwen2.5-VL-72B (F1: 0.72, BLEU: 0.46).
- ~0.35 seconds/page on a consumer GPU, <500MB VRAM.

**Granite-Docling** (IBM, 2025)
Production-ready successor to SmolDocling. Replaces SmolLM-2 backbone with Granite 3 architecture and SigLIP2 visual encoder. Outperforms SmolDocling while maintaining similar efficiency.

**MinerU 2.5-Pro** (OpenDataLab, 2025-2026)
With a 1.2B parameter VLM backbone, MinerU 2.5 achieves state-of-the-art on OmniDocBench, outperforming Gemini 2.5 Pro, GPT-4o, and Qwen2.5-VL-72B on the benchmark's comprehensive document parsing tasks. Supports images inside tables, cross-page table merging, and truncated paragraph merging.

### 4.3 Table Extraction: Specialized Models

**Table Transformer (TATR)** (Microsoft, 2022)
DETR-based (Detection Transformer) pipeline with two stages:
1. Table detection: locates table regions within a page.
2. Table structure recognition: identifies rows, columns, column headers, and spanning cells within a detected table.

Trained on PubTables-1M (scientific tables) and FinTabNet (financial tables from SEC filings). Three v1.1 model variants: PubTables, FinTabNet.c, and combined.

Benchmark results on ICDAR-2013 (after annotation correction):
- Combined training: **81% exact match accuracy** (up from 69% before annotation fix).
- PubTables-only: 75%.
- FinTabNet-only: 65%.

FinTabNet contains 112K+ tables from S&P 500 annual reports — the standard benchmark for financial document table extraction.

**TableFormer** (IBM Research)
Used inside Docling. A transformer-based model for table structure recognition. Handles complex tables with merged cells, multi-row/column headers. Reported to outperform TATR on documents with irregular structure.

**LATTE** (Purdue, AAAI 2025)
Improves LaTeX recognition for tables and formulas with iterative refinement. Addresses the common failure mode where initial recognition produces syntactically valid but semantically wrong LaTeX.

**Reducto's Agentic Table Parsing** (2024-2025)
Reducto uses a multi-step approach: baseline OCR → VLM review of results → correction agent for merged cells and complex structures. This "Agentic OCR" approach reportedly handles merged cells significantly better than single-pass models.

### 4.4 Reading Order Prediction

Reading order prediction has been reframed as a learning problem rather than a rule-based one. Recent approaches:

**Relation-based ordering** (Pattern Recognition, 2024): Predicts binary classification probabilities for pairs of text blocks as directed edges in a graph. Final reading order is determined by a path-searching algorithm over the graph. Handles complex multi-column and mixed layouts better than heuristic methods.

**Surya reading order**: Datalab's surya toolkit includes a trained reading order model that processes layout-detected regions and produces a sorted ordering. Benchmarked on diverse document types from Common Crawl.

**LLM-based reordering**: Some pipelines use the extracted text blocks' content as a signal for ordering (if a block contains "continued from previous page" it should follow the preceding block). Expensive but can resolve ambiguous layouts.

### 4.5 Hybrid Approaches

The most effective production systems (2024-2025) combine multiple techniques:

1. **Detection**: Use DocLayout-YOLO or equivalent to segment the page into typed regions (text, table, figure, formula, header, footer).
2. **Per-region extraction**: Apply the appropriate method per region type — direct text extraction for text regions, TATR/TableFormer for tables, Nougat/GOT-OCR for formulas.
3. **Reading order**: Apply a trained ordering model to sort the extracted regions.
4. **Post-processing**: Merge cross-page elements, strip headers/footers, resolve footnote references.

This hybrid approach underlies Docling, MinerU, and several commercial systems.

### 4.6 Key Models Summary

| Model | Params | Domain | Strength | Weakness |
|-------|--------|--------|----------|----------|
| Nougat | 250M (base) / 350M (large) | Academic papers | Math, structured MD | Hallucination, slow, domain-specific |
| GOT-OCR 2.0 | 580M | General OCR | Multi-task, math | Still domain-limited |
| SmolDocling | 256M | General docs | Fast, small, DocTags | Newer, less tested in prod |
| Granite-Docling | ~258M | General docs | Production-ready | IBM ecosystem |
| MinerU 2.5-Pro | 1.2B | General docs | SOTA OmniDocBench | Large for edge deployment |
| DocLayout-YOLO | ~60M | Layout detection only | Speed + accuracy | Detection only, not recognition |
| TATR v1.1 | ~60M | Tables | Financial/scientific | Merged cells, irregular |

---

## 5. Approaches for RAG and Agentic Retrieval

### 5.1 Chunking Strategies

Chunking strategy choice has outsized impact on RAG retrieval quality. Seven major strategies, with measured results:

**Fixed-size / token-based chunking**: Split at every N tokens with M-token overlap. Simple, deterministic. An NVIDIA benchmark across 5 datasets (2024) found that **page-level chunking won with 0.648 accuracy** — suggesting document structure is often preserved within pages. However, this doesn't generalize across document types. Fails at sentence boundaries; breaks within tables or code blocks.

**Sentence/paragraph-based chunking**: Split at detected sentence or paragraph boundaries. Better semantic coherence. Requires a sentence splitter that handles abbreviations, decimals, and multi-sentence footnotes correctly. LangChain's RecursiveCharacterTextSplitter with appropriate separators achieves 85-90% on standard benchmarks.

**Semantic chunking**: Embed consecutive sentence windows and split when embedding cosine similarity drops below a threshold. Produces semantically coherent chunks. `LLMSemanticChunker` achieves 0.919 recall; `ClusterSemanticChunker` achieves 0.913. More compute-intensive than character splitting.

**Structural chunking** (recommended for PDF pipelines): Use document structure as chunk boundaries — split at section headings, subsections, or natural structural units like abstract/introduction/methods. Requires structure detection (headings identified by font size/style or ML model). Produces chunks aligned to how authors actually organized content.

**Hierarchical chunking**: Maintain multiple levels of granularity — large summary chunks for coarse retrieval, fine-grained sentence chunks for precise answer extraction. The "parent document retriever" pattern: index small chunks, retrieve parent chunks for context window. `HiChunk` (arXiv 2024) formalizes this with multi-level representations. Particularly effective for documents with nested section structure.

**Element-based chunking**: Treat each layout element (paragraph, table, figure+caption, code block) as a chunk, with its type as metadata. This is what Docling, Unstructured, and MinerU produce natively. Each chunk carries metadata: page number, element type, bbox, section path. Enables type-filtered retrieval ("find tables matching X").

**Late chunking** (2024): Encode the full document (or large window) with a long-context embedding model, then slice the contextual embeddings into chunks. Each chunk embedding carries context from the surrounding document, not just the isolated text. Reduces the semantic isolation problem of pre-chunking.

**Practical guidance for PDFs**:
- For simple text-heavy documents: structural chunking with section boundaries.
- For documents with complex tables: element-level chunking (table = one chunk, with surrounding context as metadata).
- For documents requiring citation back to specific locations: element-level with bbox metadata for visual grounding.
- For mixed document types at scale: hybrid router — detect document type, apply appropriate strategy.

### 5.2 Preserving Document Structure for Retrieval

Metadata tagging is as important as chunking. Each chunk should carry:
- `page_number` — for source citation.
- `element_type` — text/table/figure/heading/list.
- `section_path` — hierarchical breadcrumb (e.g., "Chapter 3 > 3.2 Methods > 3.2.1").
- `document_id` + `filename`.
- `bbox` — normalized coordinates for visual grounding (essential for ColPali-style retrieval and highlight-based citation).

Tools that produce structured output with metadata: Docling (JSON with hierarchical DocStructure), Unstructured (ElementType tagging), Adobe PDF Extract API (comprehensive JSON with spatial info), LlamaParse (Markdown with structural markers).

Hierarchical index structures: LlamaIndex's `PropertyGraphIndex` and `HierarchicalNodeParser` enable retrievers to navigate the document hierarchy — query at the section level for broad questions, drill down to paragraph level for specific facts.

### 5.3 ColPali: Late Interaction Visual Retrieval

**ColPali** (arXiv:2407.01449, July 2024) represents a fundamentally different retrieval paradigm: skip text extraction entirely, embed pages as visual patch sequences, and retrieve using late interaction over patch embeddings.

**Architecture**: PaliGemma-3B (SigLIP visual encoder + Gemma 2B LM) processes a page image into a grid of visual patch embeddings. Each patch embedding is projected to D=128 dimensions. The ColBERT late interaction mechanism is applied: for retrieval, compute max-similarity between each query token embedding and all patch embeddings, then sum these per-query maxima.

**Why this matters**: Documents convey information through layout, tables, infographics, fonts, and visual structure. Traditional retrieval discards all of this. ColPali preserves it.

**ViDoRe benchmark results** (visual document retrieval benchmark introduced in the paper):
- ColPali substantially outperforms all text-extraction-based retrieval baselines.
- The gap is largest on visually complex tasks: InfographicVQA, ArxivQA (figures), TabFQuAD (tables).
- ColPali also outperforms baselines on text-centric tasks, making it the overall best-performing retrieval model.

**Practical implications for RAG**:
- No text extraction pipeline needed.
- Naturally handles scanned documents, charts, tables, diagrams without special-casing.
- Requires storing multi-vector embeddings per page (~1000 patch vectors per page at D=128).
- Storage overhead: roughly 4× to 10× larger index than single-vector methods.
- Compatible with Qdrant's multi-vector storage (ColPali + Qdrant integration published 2024).

**Nemotron ColEmbed V2** (NVIDIA, 2025 — arXiv:2602.03992): An improved late-interaction model building on ColPali's approach, achieving top performance on ViDoRe v2. Demonstrates the rapidly maturing ecosystem around this paradigm.

**Limitation**: ColPali retrieves pages (or page segments) not specific text snippets. The downstream reading step still requires either text extraction or a VLM to read the retrieved page. Best used in a two-stage setup: ColPali for retrieval → VLM (GPT-4o, Gemini, Claude) for reading.

### 5.4 Commercial Cloud Extraction Services

**LlamaParse** (LlamaIndex)
Positioned as a PDF parser purpose-built for RAG. Uses a combination of parsing heuristics and LLM-based structure recovery. Features: natural language instructions for parsing behavior customization, table extraction to Markdown, image extraction with optional description, section heading detection.

Benchmark results (Applied AI benchmark, 800+ documents, 2025):
- **Robustness (ChrF++): 81%** — leading all tested parsers.
- Better quality/cost ratio than frontier LLMs (10-20× cheaper than GPT-4 class models for equivalent robustness).
- Best overall choice for mixed document portfolios when cost matters.

**Azure Document Intelligence** (Microsoft)
Formerly "Form Recognizer." REST API + SDK. Multiple pretrained models: Read (OCR), Layout (structure), General Document, plus specialized models for invoices, receipts, ID cards, tax forms, business cards.

Layout model outputs: words/lines with bounding polygons, paragraphs, tables (with cell spans and confidence), selection marks, key-value pairs. Output format: JSON or Markdown.

Performance context: outperforms AWS Textract on complex documents in head-to-head tests. Strong choice for Microsoft-ecosystem enterprises. Supports hybrid/on-prem deployment via containerized Read/Layout images.

**AWS Textract**
Amazon's document analysis service. APIs: `DetectDocumentText` (basic OCR), `AnalyzeDocument` (tables, forms, queries, signatures), async `StartDocumentAnalysis` for multi-page PDFs.

Strengths: deep S3/Lambda integration, mature async batch processing, strong for invoices/receipts/forms. Weakness: less accurate than Azure on complex layouts; output requires post-processing for RAG use (raw block relationships, not clean Markdown).

**Google Document AI**
Supports 30+ specialized processors plus a general layout parser. Strong OCR quality (heir to Google's cloud OCR capabilities). Processes documents up to 2000 pages. Output is JSON with form fields, tables, and text blocks.

**Adobe PDF Extract API**
Uses Adobe Sensei AI. Outputs comprehensive structured JSON including: element type, text content, bbox, reading order index, table structure, figure paths (with renditions), formula annotations. Particularly strong for publishing workflows where structural fidelity is critical. Not the fastest or cheapest option.

**Reducto**
Fast-growing startup ($108M total funding as of 2025). Differentiator: "Agentic OCR" — a VLM reviews and corrects baseline OCR results, particularly for merged cells, handwriting, and ambiguous layouts. Focused on financial and legal document extraction. Positioned as highest-accuracy commercial option for complex documents.

**Mathpix**
Specializes in STEM content — mathematical formulas, chemical structures, scientific text. Converts math-heavy PDFs to LaTeX/Markdown. Unmatched for scientific/technical documents with heavy mathematics. Not suitable for general document extraction.

### 5.5 Open-Source Stacks in Production

**Docling + LlamaIndex** (most common as of 2025)
Docling for document parsing (hybrid heuristic + ML), LlamaIndex for indexing and retrieval. Docling produces a rich JSON DocStructure that LlamaIndex's DoclingReader consumes natively. Also integrated with LangChain, spaCy. 37,000+ GitHub stars.

**MinerU + downstream embedding**
MinerU for PDF-to-Markdown conversion with OCR and layout recovery. Output is Markdown + JSON with element metadata. Used heavily in Chinese AI research for training data construction.

**Unstructured + vector DB**
Unstructured's open-source library (`unstructured` package) provides document partitioning with element typing. Cloud API adds higher-quality chunking and enrichments. Native connectors to Pinecone, Weaviate, Qdrant, Chroma, Neo4j. Popular in enterprise RAG pipelines due to breadth of format support (PDF, DOCX, HTML, PPTX, images, emails).

**marker + downstream**
Marker (Vik Paruchuri / Datalab) converts PDF to Markdown + JSON using a heuristic-first, ML-assist approach. Uses surya for OCR and layout, PyMuPDF for text layer, and custom post-processing. Projected 25 pages/second throughput on H100 GPU. The Markdown output is clean enough for direct LLM consumption.

---

## 6. The Tools Landscape

### 6.1 Open-Source Tools

#### Text Extraction (Born-Digital)

**PyMuPDF (pymupdf / fitz)**
- **Languages**: Python (C core)
- **Speed**: Very fast — 3-15× faster than alternatives
- **License**: GNU AGPL (commercial license available from Artifex)
- **Strengths**: Fastest Python PDF library; `pymupdf4llm` for LLM-ready Markdown; rich API (rendering, TOC, links, images); handles large files well
- **Weaknesses**: AGPL can be a license blocker; encoding issues with some CJK documents
- **Best for**: Production born-digital PDF extraction, high-throughput pipelines

**pdfplumber**
- **Languages**: Python
- **Speed**: Moderate (built on pdfminer.six)
- **License**: MIT
- **Strengths**: Best-in-class table extraction for born-digital PDFs; visual debugging; exposes raw character-level data
- **Weaknesses**: Slow for large volumes; rule-based table detection fails on borderless or irregular tables
- **Best for**: Documents with structured tables where visual alignment matters

**pdftext** (Datalab)
- **Languages**: Python
- **Speed**: Fast (pypdfium2 backend)
- **License**: Apache 2.0
- **Strengths**: Structured block/line output; fast; used internally by marker
- **Weaknesses**: Less widely used than PyMuPDF; fewer features
- **Best for**: When Apache license is required; marker pipeline dependency

**pypdf**
- **Languages**: Python
- **Speed**: Moderate
- **License**: BSD-3-Clause
- **Strengths**: Zero dependencies; simple API; good for merge/split/metadata
- **Weaknesses**: Lower text extraction quality; known CMap bugs
- **Best for**: Simple PDF manipulation, not text extraction

#### OCR and Layout

**Surya** (Datalab / Vik Paruchuri)
- A unified toolkit: OCR (90+ languages), layout analysis, reading order prediction, table recognition
- Benchmarked against Google Cloud Vision for OCR quality
- Layout benchmarked on PubLayNet (not in training data) showing strong generalization
- Used as the OCR engine inside marker
- Models available on HuggingFace

**PaddleOCR** (Baidu)
- Complete OCR pipeline with detection + recognition + table parsing
- Strong performance on complex layouts
- 80+ language support
- Apache 2.0 license
- `paddleocr` Python package; well-documented

**Tesseract** (Google)
- Version 5 with LSTM engine
- 89-94% accuracy on clean scans; degrades significantly on noise
- Weak multi-column handling
- `pytesseract` Python wrapper; `OCRmyPDF` for PDF-specific workflows
- Apache 2.0 license

#### Full Pipelines

**Docling** (IBM / docling-project)
- Architecture: DocLayout-YOLO / TableFormer / SmolDocling
- Input: PDF, DOCX, PPTX, XLSX, HTML, images, audio
- Output: Markdown, JSON (hierarchical DocStructure), DocTags
- Features: table structure recovery, formula detection, image classification, reading order
- License: MIT
- Integration: LlamaIndex, LangChain, spaCy, Haystack
- 37,000+ GitHub stars (Nov 2024 to present)
- Production-grade, low memory footprint ("runs on commodity hardware")

**MinerU** (OpenDataLab)
- Architecture: DocLayout-YOLO for layout, Paddle for OCR, specialized VLM (1.2B) in MinerU 2.5
- Output: Markdown + JSON with bbox metadata
- Features: multi-format (PDF, DOCX, PPTX, XLSX), cross-page table merging, image recognition inside tables
- License: MinerU custom open-source license (Apache 2.0 based, as of 2025)
- SOTA on OmniDocBench (MinerU 2.5 surpasses GPT-4o, Gemini 2.5 Pro)
- Primarily developed for Chinese AI research; strong CJK support

**marker** (Datalab / Vik Paruchuri)
- Architecture: surya (OCR + layout) + PyMuPDF (text layer) + post-processing models
- Output: Markdown + JSON with element metadata
- Features: table to Markdown, image extraction, code block detection, formula handling
- Speed: up to 25 pages/second on H100 (batch mode)
- License: GPL-3.0 (non-commercial); commercial license available
- Very widely used for RAG data preparation

**unstructured.io** (Unstructured)
- Architecture: detection model + direct extraction + OCR fallback
- Output: typed elements (Title, NarrativeText, Table, Image, etc.) with metadata
- Features: connectors to 20+ data sources (S3, GDrive, Salesforce); vector DB sinks; enterprise platform
- License: Apache 2.0 (open-source library); commercial platform available
- Broad format support (25+ file types)
- Strong for heterogeneous document ingestion across an organization

**Nougat** (Meta AI)
- Best-in-class for academic paper extraction (math, structured content)
- Limited generalization to non-arXiv documents
- Hallucination rate a concern in production
- Use when scientific papers with heavy math are the primary input

**PDF-Extract-Kit** (OpenDataLab)
- Comprehensive modular toolkit combining: DocLayout-YOLO, DOCLAYOUT, MFR (formula recognition), MFD (formula detection), table detection/recognition
- More customizable than MinerU; used to build custom extraction pipelines

#### Benchmarks and Evaluation

**DocLayNet** (IBM, KDD 2022): 80,863 pages from 6 domains (Finance, Science, Patents, Law, Tenders, Manuals), 11 layout classes. The most diverse layout benchmark; models trained on it generalize better than PubLayNet-trained models. mAP baseline ~81.6%.

**PubLayNet** (IBM, ICDAR 2019): 360,000 pages from PubMed articles. Large but domain-limited (academic papers only). mAP ~97.3% achievable with modern models.

**FinTabNet**: 112K+ tables from S&P 500 annual reports. Standard for financial table extraction. TATR v1.1-fin achieves 65-81% exact match depending on test set.

**OmniDocBench** (CVPR 2025, arXiv:2412.07626): 1,355 pages from 9 document types (academic, financial, newspapers, textbooks, handwritten), 15 block-level + 4 span-level annotation categories, 4 layout types, 3 language types. The most comprehensive end-to-end parsing benchmark as of 2025.

**DocStructBench** (introduced in DocLayout-YOLO paper, 2024): Complex, challenging layout detection benchmark across diverse document types.

### 6.2 Commercial/Cloud Tools Comparison

| Tool | Approach | Best For | Cost (est.) | License |
|------|----------|---------|-------------|---------|
| LlamaParse | LLM-assisted | RAG, mixed docs | ~$0.003/page | Commercial |
| Azure Doc Intelligence | CNN/Transformer | Enterprise, forms | ~$0.001-0.010/page | Commercial |
| AWS Textract | CNN/Transformer | AWS-native, forms | ~$0.001-0.015/page | Commercial |
| Google Document AI | Google OCR | High-volume OCR | ~$0.001-0.010/page | Commercial |
| Adobe PDF Extract | Sensei AI | Publishing, legal | ~$0.005-0.050/page | Commercial |
| Reducto | Agentic OCR | Financial, legal | Custom/usage | Commercial |
| Mathpix | STEM-specific OCR | Math, science | ~$0.004/page | Commercial |

*Cost estimates are approximate and vary by volume, tier, and document complexity. Verify with provider pricing pages.*

### 6.3 Quality vs. Speed vs. Cost Trade-offs

**For high-throughput born-digital pipelines** (millions of pages, mostly text):
→ PyMuPDF / pdftext — fastest, near-zero cost, excellent for clean documents.

**For mixed document quality (some scans, some complex layout)**:
→ Docling or marker — handles both cases with a single pipeline; strong open-source option.

**For scientific/research paper extraction**:
→ Nougat (if hallucination acceptable) or MinerU 2.5 (better generalization).

**For financial documents (tables, reports)**:
→ TATR v1.1-fin + PyMuPDF, or Reducto/Azure Document Intelligence for commercial.

**For maximum accuracy regardless of cost**:
→ Frontier LLM (Gemini 3 Pro or GPT-5.1 in multimodal mode): GPT-5.1 achieves 92% edit similarity — 14 points above best open-source. Cost: ~$0.01-0.05/page.

**For retrieval without text extraction**:
→ ColPali + ColBERT late interaction; retrieve pages visually, read with VLM.

Applied AI benchmark (800+ documents, 2025) summary:
- Parser accuracy varies by **55+ percentage points** depending on document type.
- Legal contracts: up to 95% accuracy with good parsers.
- Academic papers: 40-60% even with frontier models.
- No single parser wins across all domains — a routing system is optimal at scale.

---

## 7. Current State of the Art and Open Problems

### 7.1 What the Benchmarks Show (2025)

**OmniDocBench (CVPR 2025)** evaluated both pipeline-based methods and end-to-end VLMs across 9 document types. Key findings:
- Even the strongest models show consistent drops on multi-column and complex mixed layouts.
- Handwritten notes and densely typeset newspapers remain significantly harder than academic papers.
- End-to-end VLMs show promise but don't uniformly dominate pipeline approaches — each has domains where it excels.
- MinerU 2.5-Pro (1.2B parameter VLM) achieves the top reported scores as of early 2026.

**Applied AI benchmark (800+ documents, 2025)**:
- GPT-5.1 (multimodal) achieves 92% edit similarity overall — best tested.
- Best open-source: 78% — a meaningful gap remains.
- ArXiv papers with equations remain the hardest category at 40-60% for all parsers.
- Document type matters more than parser choice for accuracy.

### 7.2 Open Problems

**Complex tables with merged cells.** The single hardest remaining problem in PDF parsing. Cells that span multiple rows or columns, nested tables, projected row headers, and borderless tables with irregular alignment all defeat current approaches. Even Reducto's Agentic OCR and frontier LLMs struggle with pathological cases from financial/legal documents. Research benchmark FinTabNet still shows significant gaps between model output and ground truth for complex structures.

**Mathematical notation at scale.** Formula extraction accuracy degrades rapidly with complexity. Multi-line formulas, aligned equations, and formulas with complex nesting are near-perfectly handled by Mathpix (specialist) but poorly by general parsers. No open-source tool matches Mathpix quality on complex math.

**Mixed-language and bidi documents.** Arabic/Hebrew mixed with Latin text, Indic scripts, and CJK vertical text remain underserved. Most English-centric models degrade significantly. PaddleOCR has the best CJK support; Arabic support lags everywhere.

**Reading order in complex layouts.** Magazine-style layouts, documents with sidebars, content that spans irregular regions — reading order prediction degrades significantly. The ML-based approaches (Surya, relation-prediction models) represent genuine progress but still have failure modes.

**Cross-page element merging.** A table split across pages, a paragraph interrupted by a page break, a figure with a caption on the following page — these are reliably handled by essentially no tool outside MinerU 2.5's explicit cross-page table merging. This is critical for long technical documents.

**Code extraction.** Indentation, syntax, and line breaks in code blocks are often mangled. Only tools with explicit code block detection (marker, Docling via code element type) handle this at all.

**Scanned document quality.** The gap between born-digital and scanned extraction is significant. For scanned documents with noise, skew, or low resolution, even the best OCR approaches lose 15-30% accuracy compared to their clean-scan performance.

**Hallucination in VLM-based extraction.** Autoregressive decoders (Nougat, GOT-OCR) can generate plausible-looking text not present in the document. This is an existential problem for RAG pipelines where factual grounding is critical. Detection and mitigation remain active research areas.

**Scale and latency.** VLM-based approaches are dramatically slower than text-layer extraction. At 5-20 seconds/page for Nougat vs. sub-millisecond for PyMuPDF on born-digital, there is a fundamental compute cost gap. MinerU 2.5's 1.2B model and SmolDocling's 256M model represent efforts to close this gap while maintaining quality.

### 7.3 Where the Field Is Heading

**Native multimodal end-to-end pipelines.** The trend is clear: unified VLMs that take a document image and produce structured output (Markdown, JSON, DocTags) are rapidly improving. MinerU 2.5's SOTA OmniDocBench results with 1.2B parameters suggest this will replace hybrid pipelines for many use cases within 2-3 years.

**Page-level embeddings for retrieval.** ColPali and its successors (Nemotron ColEmbed V2) demonstrate that visual page embeddings without text extraction can outperform text-extraction-based retrieval. Expect this to become a standard RAG architecture for visually-rich document collections.

**Smaller, faster models.** SmolDocling (256M) outperforming models 27× its size is emblematic of the efficiency gains being achieved. Distillation from larger models, synthetic data (DocSynth-300K), and architectural improvements are pushing performance down to consumer GPU levels.

**Agentic verification loops.** Reducto's "Agentic OCR" pattern (extract → verify → correct with VLM) is likely to propagate. For high-stakes extraction (financial tables, legal clauses), an agent that checks and fixes initial extraction results is more reliable than any single-pass model.

**Unified document understanding models.** DocFusion (2025) achieves OCR, table recognition, and math expression recognition within a single 289M-parameter model. This convergence toward unified models reduces pipeline complexity.

**Layout-aware embeddings for RAG.** Rather than discarding layout after extraction, future embedding models will incorporate spatial information (element type, bbox, section hierarchy) into retrieval-optimized representations. Early work on "layout-aware BERT" variants points in this direction.

**Better benchmarks driving progress.** OmniDocBench (CVPR 2025) is already forcing more comprehensive evaluation. Expect benchmarks covering agentic workflows end-to-end (not just extraction accuracy) — measuring what matters for RAG: answer grounding, hallucination rate, citation accuracy.

---

## 8. Recommendations by Use Case

### Simple text extraction from born-digital PDFs at scale
**PyMuPDF + pymupdf4llm.** Zero compromise on speed; Markdown output; handles the large majority of document types correctly. Only consider alternatives when you hit encoding edge cases.

### Mixed born-digital and scanned, general documents
**Docling** (open source) or **marker** (faster, GPL-licensed). Both handle the scanned/digital split transparently. Docling for structured JSON output; marker for clean Markdown.

### Scientific papers with heavy math
**MinerU** (if open-source required) or **Mathpix** (best formula accuracy, commercial). Nougat is now somewhat dated given MinerU 2.5's superiority on OmniDocBench.

### Financial documents, SEC filings, annual reports
**TATR v1.1-fin** + PyMuPDF for open-source. **Reducto** or **Azure Document Intelligence** for commercial. FinTabNet-trained models are essential.

### RAG pipeline requiring source citation and visual grounding
**Docling** or **Adobe PDF Extract API** (rich bbox metadata) + hierarchical chunking with section path metadata. Combine with ColPali-style retrieval for visual documents.

### Fully visual retrieval (no text extraction)
**ColPali** (2407.01449) + Qdrant multi-vector index. Best for collections with infographics, charts, mixed visual/text documents. Two-stage: ColPali retrieval → multimodal LLM reading.

### Maximum accuracy, cost not a constraint
**Frontier multimodal LLMs** (Gemini 3 Pro, GPT-5.1) in document reading mode — 14-20 points better than best open-source. Use a quality classifier to route only hard documents to expensive models.

### High-volume enterprise pipeline with compliance requirements
**Azure Document Intelligence** (Microsoft ecosystem) or **AWS Textract** (AWS ecosystem). Both offer SLAs, audit trails, and data residency options.

---

## Key References

- DocLayout-YOLO: arXiv:2410.12628 (Oct 2024)
- ColPali: arXiv:2407.01449 (Jul 2024)
- OmniDocBench: arXiv:2412.07626 (Dec 2024, CVPR 2025)
- Nougat: arXiv:2308.13418 (Aug 2023, ICLR 2024)
- Comparative PDF Parsing Study: arXiv:2410.09871 (Oct 2024)
- SmolDocling: arXiv:2503.11576 (Mar 2025)
- Nemotron ColEmbed V2: arXiv:2602.03992 (2025)
- TATR Table Transformer: github.com/microsoft/table-transformer
- Docling: github.com/docling-project/docling
- MinerU: github.com/opendatalab/MinerU
- marker: github.com/datalab-to/marker
- surya: github.com/datalab-to/surya
- Applied AI PDF parsing benchmark: applied-ai.com/briefings/pdf-parsing-benchmark/
- Reducto parser comparison: llms.reducto.ai/document-parser-comparison
- DocLayNet: arXiv:2206.01062
