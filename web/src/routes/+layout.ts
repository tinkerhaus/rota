// SPA mode: no server-side rendering, no prerendering. Every route resolves
// against the fallback index.html and renders on the client.
export const ssr = false;
export const prerender = false;
export const trailingSlash = 'never';
