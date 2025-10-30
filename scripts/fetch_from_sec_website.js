import fs from "node:fs";
import { JSDOM } from "jsdom";

let keyword = "sonm";

async function downloadWebpage(url) {
  // Fetch from this url
  const headers = new Headers();
  headers.append("User-Agent", "LifeProtect24/7 jon@protect247.com");

  let res = await fetch(url, {
    headers,
  });
  let html = await res.text();

  Bun.write(`./sec-docs/${keyword}.html`, html);
}

function extractTablesOnly(inputFilePath, outputFilePath) {
  try {
    // Read the HTML file
    const htmlContent = fs.readFileSync(inputFilePath, "utf8");

    // Parse the HTML using JSDOM
    const dom = new JSDOM(htmlContent);
    const document = dom.window.document;

    // Find all table elements
    const tables = document.querySelectorAll("table");

    // Create a new document with only tables
    const newDocument = new JSDOM(
      "<!DOCTYPE html><html><head></head><body></body></html>"
    );
    const newBody = newDocument.window.document.body;

    // Clone each table and append to new document
    tables.forEach((table) => {
      const clonedTable = table.cloneNode(true);
      newBody.appendChild(clonedTable);
    });

    // Get the final HTML
    const finalHtml = newDocument.serialize();

    // Write to output file
    fs.writeFileSync(outputFilePath, finalHtml);

    console.log(
      `Successfully extracted ${tables.length} table(s) from ${inputFilePath} to ${outputFilePath}`
    );
  } catch (error) {
    console.error("Error processing file:", error.message);
  }
}

// Usage - replace with your actual file paths
// extractTablesOnly("./prospectus.html", "pros-output.html");

downloadWebpage(
  "https://www.sec.gov/Archives/edgar/data/1178697/000164117225017383/form424b4.htm"
);
