# Vercel Deployment (Static Frontend)

This folder is a static frontend. Vercel will deploy it without a build step.

## Deploy in Vercel (UI)
1. Create a new project and select the repo `ED-TECH-PORTAL`.
2. Set **Root Directory** to `Classbridge/class/frontend/static_app`.
3. Framework preset: **Other** (or **No Framework**).
4. Build Command: **leave empty**.
5. Output Directory: **leave empty**.
6. Deploy.

`vercel.json` in this folder already configures static serving and SPA fallback.

## Localhost Check
From the repo root:

```bash
python3 -m http.server 5173 --directory Classbridge/class/frontend/static_app
```

Then open: `http://localhost:5173`

If assets do not load, confirm this folder is the project root in Vercel.
