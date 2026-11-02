package ui

import "embed"

//go:embed index.html style.css view.js app.js
var Files embed.FS
