// Mixed read-heavy workload: ~100 redirects per link creation.
//
//   make loadtest                      # defaults: 1500 redirects/s for 60s
//   make loadtest RATE=3000 DURATION=2m
//
// Slug popularity is skewed (a few hot links get most traffic), like real
// shorteners; a small share of requests hit unknown slugs to exercise the
// negative cache. k6 is told NOT to follow redirects: we measure our 302.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE || 'http://localhost:8080';
const KEY = __ENV.API_KEY || 'dev-key-change-me';
const RATE = parseInt(__ENV.RATE || '1500');          // redirects per second
const DURATION = __ENV.DURATION || '60s';
const SEED_LINKS = parseInt(__ENV.SEED_LINKS || '500');

const unexpected = new Counter('unexpected_status');

export const options = {
  discardResponseBodies: true,
  scenarios: {
    redirects: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 100, maxVUs: 1000,
      exec: 'redirect',
    },
    creates: {
      executor: 'constant-arrival-rate',
      rate: Math.max(1, Math.floor(RATE / 100)), timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 10, maxVUs: 100,
      exec: 'create',
    },
    unknown: {
      executor: 'constant-arrival-rate',
      rate: Math.max(1, Math.floor(RATE / 50)), timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 10, maxVUs: 100,
      exec: 'unknown',
    },
  },
  thresholds: {
    'http_req_duration{scenario:redirects}': ['p(95)<50', 'p(99)<100'],
    'http_req_duration{scenario:creates}': ['p(95)<250'],
    'http_req_failed{scenario:redirects}': ['rate<0.001'],
    'checks': ['rate>0.999'],
    'unexpected_status': ['count<5'],
  },
};

export function setup() {
  const slugs = [];
  const hdr = { responseType: 'text', headers: { 'X-API-Key': KEY, 'Content-Type': 'application/json' } };
  for (let i = 0; i < SEED_LINKS; i++) {
    const r = http.post(`${BASE}/v1/links`, JSON.stringify({ url: `https://example.com/seed/${i}` }), hdr);
    if (r.status !== 201) {
      throw new Error(`seed create failed: ${r.status}. Raise RATE_LIMIT_* (make loadtest does this).`);
    }
    slugs.push(r.json('slug'));
  }
  return { slugs };
}

// Skewed pick: u^3 concentrates ~50% of traffic on the first ~13% of links.
function pick(slugs) {
  return slugs[Math.floor(slugs.length * Math.pow(Math.random(), 3))];
}

export function redirect(data) {
  const r = http.get(`${BASE}/${pick(data.slugs)}`, {
    redirects: 0,
    headers: { Referer: 'https://loadtest.example/page', 'User-Agent': 'k6-loadtest' },
  });
  if (!check(r, { 'redirect is 302': (res) => res.status === 302 })) unexpected.add(1);
}

export function create() {
  const r = http.post(`${BASE}/v1/links`,
    JSON.stringify({ url: `https://example.com/load/${__VU}/${__ITER}/${Date.now()}` }),
    { headers: { 'X-API-Key': KEY, 'Content-Type': 'application/json' } });
  if (!check(r, { 'create is 201': (res) => res.status === 201 })) unexpected.add(1);
}

export function unknown() {
  const r = http.get(`${BASE}/nope${Math.floor(Math.random() * 1e6)}`, {
    redirects: 0,
    responseCallback: http.expectedStatuses(404), // a 404 is the correct answer here
  });
  if (!check(r, { 'unknown is 404': (res) => res.status === 404 })) unexpected.add(1);
}
