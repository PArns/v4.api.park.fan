import { Injectable } from "@nestjs/common";
import { marked } from "marked";
import { readFileSync } from "fs";
import { join } from "path";

@Injectable()
export class AppService {
  private cachedHtml: string | null = null;

  /**
   * Get README.md content as styled HTML
   * Uses in-memory cache for performance
   */
  getReadmeAsHtml(): string {
    if (this.cachedHtml) {
      return this.cachedHtml;
    }

    try {
      // Read README.md from project root
      const readmePath = join(process.cwd(), "README.md");
      const markdown = readFileSync(readmePath, "utf-8");

      // Convert Markdown to HTML
      const contentHtml = marked.parse(markdown) as string;

      // Wrap in styled HTML template
      this.cachedHtml = this.wrapInHtmlTemplate(contentHtml);

      return this.cachedHtml;
    } catch (error) {
      console.error("Error reading README.md:", error);
      return this.getFallbackHtml();
    }
  }

  /**
   * Wrap markdown content in a styled HTML template
   */
  private wrapInHtmlTemplate(content: string): string {
    return `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>park.fan API v4 - Documentation</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
            line-height: 1.6;
            color: #24292e;
            background-color: #ffffff;
            padding: 20px;
            max-width: 1200px;
            margin: 0 auto;
        }

        h1, h2, h3, h4, h5, h6 {
            margin-top: 24px;
            margin-bottom: 16px;
            font-weight: 600;
            line-height: 1.25;
        }

        h1 {
            font-size: 2.5em;
            padding-bottom: 0.3em;
            border-bottom: 1px solid #eaecef;
        }

        h2 {
            font-size: 2em;
            padding-bottom: 0.3em;
            border-bottom: 1px solid #eaecef;
        }

        h3 {
            font-size: 1.5em;
        }

        p {
            margin-bottom: 16px;
        }

        a {
            color: #0366d6;
            text-decoration: none;
        }

        a:hover {
            text-decoration: underline;
        }

        code {
            background-color: #f6f8fa;
            padding: 0.2em 0.4em;
            margin: 0;
            font-size: 85%;
            border-radius: 3px;
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
        }

        pre {
            background-color: #f6f8fa;
            padding: 16px;
            overflow: auto;
            font-size: 85%;
            line-height: 1.45;
            border-radius: 6px;
            margin-bottom: 16px;
        }

        pre code {
            background-color: transparent;
            padding: 0;
            margin: 0;
            font-size: 100%;
            border-radius: 0;
            display: block;
        }

        table {
            border-spacing: 0;
            border-collapse: collapse;
            margin-bottom: 16px;
            width: 100%;
        }

        table th, table td {
            padding: 6px 13px;
            border: 1px solid #dfe2e5;
        }

        table th {
            font-weight: 600;
            background-color: #f6f8fa;
        }

        table tr:nth-child(2n) {
            background-color: #f6f8fa;
        }

        blockquote {
            padding: 0 1em;
            color: #6a737d;
            border-left: 0.25em solid #dfe2e5;
            margin-bottom: 16px;
        }

        ul, ol {
            padding-left: 2em;
            margin-bottom: 16px;
        }

        li {
            margin-bottom: 8px;
        }

        hr {
            height: 0.25em;
            padding: 0;
            margin: 24px 0;
            background-color: #e1e4e8;
            border: 0;
        }

        img {
            max-width: 100%;
            height: auto;
        }

        .badge {
            display: inline-block;
            margin: 2px;
        }

        /* Center div alignment */
        div[align="center"] {
            text-align: center;
        }

        /* Dark mode support */
        @media (prefers-color-scheme: dark) {
            body {
                background-color: #0d1117;
                color: #c9d1d9;
            }

            h1, h2 {
                border-bottom-color: #21262d;
            }

            a {
                color: #58a6ff;
            }

            code {
                background-color: #161b22;
            }

            pre {
                background-color: #161b22;
            }

            table th, table td {
                border-color: #30363d;
            }

            table th {
                background-color: #161b22;
            }

            table tr:nth-child(2n) {
                background-color: #161b22;
            }

            hr {
                background-color: #21262d;
            }

            blockquote {
                color: #8b949e;
                border-left-color: #3b434b;
            }
        }

        /* Responsive design */
        @media (max-width: 768px) {
            body {
                padding: 10px;
            }

            h1 {
                font-size: 2em;
            }

            h2 {
                font-size: 1.5em;
            }

            h3 {
                font-size: 1.25em;
            }
        }
    </style>
</head>
<body>
    ${content}
    
    <hr style="margin-top: 48px;">
    <footer style="text-align: center; color: #6a737d; font-size: 0.9em; margin-top: 24px;">
        <p>
            🚀 <strong>park.fan API v4</strong> — 
            <a href="/v1">API Base</a> · 
            <a href="/api">Swagger Docs</a>
        </p>
        <p style="margin-top: 8px;">
            Powered by NestJS · TypeScript · PostgreSQL · Redis
        </p>
    </footer>
</body>
</html>
    `.trim();
  }

  /**
   * Fallback HTML if README.md cannot be read
   */
  private getFallbackHtml(): string {
    return `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>park.fan API v4</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            text-align: center;
        }
        .container {
            max-width: 600px;
            padding: 40px;
        }
        h1 {
            font-size: 3em;
            margin-bottom: 20px;
        }
        p {
            font-size: 1.2em;
            margin-bottom: 30px;
        }
        a {
            color: white;
            text-decoration: none;
            padding: 12px 24px;
            background: rgba(255, 255, 255, 0.2);
            border-radius: 6px;
            margin: 0 10px;
            display: inline-block;
            margin-top: 10px;
            transition: background 0.3s;
        }
        a:hover {
            background: rgba(255, 255, 255, 0.3);
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎢 park.fan API v4</h1>
        <p>Real-time theme park intelligence powered by machine learning</p>
        <div>
            <a href="/v1">API Base</a>
            <a href="/api">API Documentation</a>
            <a href="http://localhost:3001" target="_blank">Bull Board</a>
        </div>
    </div>
</body>
</html>
    `.trim();
  }
}
