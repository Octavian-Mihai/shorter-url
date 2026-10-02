// Tiny sanity run, suitable for CI: 20 redirects/s for 10s.
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE || 'http://localhost:8080';
const KEY = __ENV.API_KEY || 'dev-key-change-me';

export const options = {
  vus: 5, duration: '10s',
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<250'] },
};

export function setup() {
  const r = http.post(`${BASE}/v1/links`, JSON.stringify({ url: 'https://example.com/k6' }),
    { headers: { 'X-API-Key': KEY, 'Content-Type': 'application/json' } });
  check(r, { created: (res) => res.status === 201 });
  return { slug: r.json('slug') };
}

export default function (data) {
  const r = http.get(`${BASE}/${data.slug}`, { redirects: 0 });
  check(r, { '302': (res) => res.status === 302 });
}
