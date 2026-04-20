const fs = require('fs');
const path = require('path');

const publicPdfjsDir = path.resolve(__dirname, '../public/pdfjs');
const buildDir = path.join(publicPdfjsDir, 'build');
const webDir = path.join(publicPdfjsDir, 'web');

console.log('PDF.js assets copy script');
console.log('Checking for viewer.html in', webDir);

if (fs.existsSync(path.join(webDir, 'viewer.html'))) {
  console.log('PDF.js viewer already exists in public/pdfjs.');
  process.exit(0);
}

// In a real scenario, this script would copy from node_modules if they existed there,
// or fail with instructions. Since we manually downloaded them, we just verify they exist.

console.error('PDF.js viewer.html not found in public/pdfjs/web/');
console.error('Please ensure PDF.js generic build is extracted into public/pdfjs/');
process.exit(1);
