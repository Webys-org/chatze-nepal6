/**
 * Chatze Nepal Edition - Cloudflare Edge Worker
 * 
 * Implements SYSTEM_DESIGN_CLOUDFLARE_ZERO_SETUP.md:
 * - 100% Native Cloudflare Worker entrypoint with zero external dependencies
 * - Binds directly to Cloudflare D1 Native Database (env.DB)
 * - 100-Second SSE Real-Time Streaming with Delta-Polling Fallback
 * - First-Launch 60-Second Business Setup Wizard
 * - Full Authentication (Admin Setup, Sign In, Sign Up, Session)
 * - Real-Time Customer Messaging with Nepal Quick Canned Responses
 * - Zero-Config Asymmetric WebCrypto Federation (ECDSA P-256)
 * - Nightly 90-Day Retention Scheduled Cron
 */

const D1_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS system_config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "user" (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  passwordHash TEXT,
  emailVerified INTEGER NOT NULL DEFAULT 0,
  image TEXT,
  createdAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS session (
  id TEXT PRIMARY KEY NOT NULL,
  expiresAt TIMESTAMP NOT NULL,
  token TEXT NOT NULL UNIQUE,
  createdAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ipAddress TEXT,
  userAgent TEXT,
  userId TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS profiles (
  userId TEXT PRIMARY KEY NOT NULL,
  username TEXT NOT NULL UNIQUE,
  displayName TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'customer',
  createdAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS conversations (
  id TEXT PRIMARY KEY NOT NULL,
  userAId TEXT NOT NULL,
  userBId TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  lastMessageSnippet TEXT,
  lastMessageAt TIMESTAMP,
  createdAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (userAId, userBId)
);

CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY NOT NULL,
  conversationId TEXT NOT NULL,
  senderId TEXT NOT NULL,
  body TEXT NOT NULL,
  createdAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS federation_friendships (
  id TEXT PRIMARY KEY NOT NULL,
  localUserId TEXT NOT NULL,
  remotePeerUrl TEXT NOT NULL,
  remoteHandle TEXT NOT NULL,
  remotePublicKey TEXT NOT NULL,
  symmetricKeyEncrypted TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  createdAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_messages_conv_created ON messages(conversationId, createdAt DESC);
CREATE INDEX IF NOT EXISTS idx_conversations_user_a ON conversations(userAId, createdAt DESC);
CREATE INDEX IF NOT EXISTS idx_conversations_user_b ON conversations(userBId, createdAt DESC);
`

let dbMigrated = false

async function ensureD1Tables(db) {
  if (dbMigrated || !db) return
  try {
    const statements = D1_SCHEMA_SQL.trim()
      .split(';')
      .map((s) => s.trim())
      .filter(Boolean)
    for (const stmt of statements) {
      await db.prepare(stmt).run().catch(() => {})
    }
    dbMigrated = true
  } catch (err) {
    console.warn('[D1 Worker Init]', err)
  }
}

// In-memory pub/sub for real-time dispatch across worker requests
const activeStreams = new Map()

function broadcastUserEvent(userId, event) {
  const listeners = activeStreams.get(userId)
  if (listeners) {
    const payload = `event: event\ndata: ${JSON.stringify(event)}\n\n`
    for (const send of listeners) {
      try {
        send(payload)
      } catch {}
    }
  }
}

function parseCookies(cookieHeader) {
  const list = {}
  if (!cookieHeader) return list
  cookieHeader.split(';').forEach((cookie) => {
    let [name, ...rest] = cookie.split('=')
    name = name?.trim()
    if (!name) return
    const value = rest.join('=').trim()
    list[name] = decodeURIComponent(value)
  })
  return list
}

async function getUserFromRequest(request, env) {
  const cookies = parseCookies(request.headers.get('Cookie'))
  const token = cookies.chatze_session
  if (!token || !env.DB) return null
  try {
    const sess = await env.DB.prepare(
      'SELECT s.userId, u.name, u.email, p.username, p.displayName, p.role FROM session s JOIN "user" u ON s.userId = u.id JOIN profiles p ON s.userId = p.userId WHERE s.token = ?'
    ).bind(token).first()
    return sess || null
  } catch {
    return null
  }
}

export default {
  async fetch(request, env, ctx) {
    if (env.DB) {
      globalThis.env = env
      await ensureD1Tables(env.DB)
    }

    const url = new URL(request.url)
    const pathname = url.pathname

    // 1. Health & Discovery Endpoint (/api/health)
    if (pathname === '/api/health') {
      return new Response(JSON.stringify({
        status: 'ok',
        platform: 'Cloudflare Workers (Kathmandu KTM Edge)',
        d1Ready: Boolean(env.DB),
        timestamp: Date.now()
      }), {
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      })
    }

    // 2. Real-time Server-Sent Events Endpoint (/api/stream)
    if (pathname === '/api/stream' && request.method === 'GET') {
      const cfRay = request.headers.get('cf-ray') || ''
      const edgeRegion = cfRay ? `KTM-CF-${cfRay.slice(-4).toUpperCase()}` : 'Kathmandu (KTM) Edge'
      const since = url.searchParams.get('since')
      const userId = url.searchParams.get('userId') || 'current'

      let pingTimer = null
      let cycleTimer = null
      let sendFn = null

      const stream = new ReadableStream({
        async start(controller) {
          const encoder = new TextEncoder()
          sendFn = (text) => {
            try { controller.enqueue(encoder.encode(text)) } catch {}
          }

          // Initial Handshake
          sendFn(`event: ready\ndata: ${JSON.stringify({
            status: 'connected',
            edgeRegion,
            clientUserId: userId,
            maxDurationSec: 100,
            ts: Date.now()
          })}\n\n`)

          // Delta recovery if client passed timestamp
          if (since && env.DB) {
            try {
              const rows = await env.DB.prepare(
                'SELECT * FROM messages WHERE createdAt > ? ORDER BY createdAt ASC LIMIT 50'
              ).bind(new Date(Number(since)).toISOString()).all()
              if (rows?.results?.length) {
                for (const msg of rows.results) {
                  sendFn(`event: event\ndata: ${JSON.stringify({
                    type: 'message',
                    message: msg,
                    conversationId: msg.conversationId
                  })}\n\n`)
                }
              }
            } catch {}
          }

          // Register in active listeners
          if (!activeStreams.has(userId)) activeStreams.set(userId, new Set())
          activeStreams.get(userId).add(sendFn)

          // 8-second keepalive ping
          pingTimer = setInterval(() => {
            sendFn(`event: ping\ndata: ${Date.now()}\n\n: ping\n\n`)
          }, 8000)

          // 95-second Cloudflare Workers graceful cycling
          cycleTimer = setTimeout(() => {
            sendFn(`event: cycle\ndata: ${JSON.stringify({ reconnect: true, ts: Date.now() })}\n\n`)
            try { controller.close() } catch {}
          }, 95000)
        },
        cancel() {
          if (pingTimer) clearInterval(pingTimer)
          if (cycleTimer) clearTimeout(cycleTimer)
          if (activeStreams.has(userId) && sendFn) {
            activeStreams.get(userId).delete(sendFn)
          }
        }
      })

      return new Response(stream, {
        headers: {
          'Content-Type': 'text/event-stream; charset=utf-8',
          'Cache-Control': 'no-cache, no-transform, no-store',
          'Connection': 'keep-alive',
          'X-Accel-Buffering': 'no',
          'X-Edge-Region': edgeRegion,
          'Access-Control-Allow-Origin': '*'
        }
      })
    }

    // 3. First-Launch Setup Wizard API (/api/setup)
    if (pathname === '/api/setup') {
      if (request.method === 'GET') {
        let isInitialized = false
        let instanceName = 'Chatze Nepal Edition'
        let adminHandle = 'admin'
        if (env.DB) {
          try {
            const countRes = await env.DB.prepare('SELECT COUNT(*) as cnt FROM profiles').first()
            isInitialized = (countRes?.cnt || 0) > 0
            const nameRow = await env.DB.prepare("SELECT value FROM system_config WHERE key = 'instance_name'").first()
            if (nameRow?.value) instanceName = nameRow.value
            const adminRow = await env.DB.prepare("SELECT username FROM profiles WHERE role = 'admin' LIMIT 1").first()
            if (adminRow?.username) adminHandle = adminRow.username
          } catch {}
        }
        return new Response(JSON.stringify({
          initialized: isInitialized,
          instanceName,
          adminHandle,
          storageEngine: 'Cloudflare D1 Native Database',
          edgeRegion: 'Kathmandu (KTM) Edge',
          version: '1.0.0-nepal-edge'
        }), {
          headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
        })
      }

      if (request.method === 'POST') {
        const body = await request.json().catch(() => ({}))
        const businessName = (body.businessName || 'Nepal Business Hub').trim()
        const adminHandle = (body.adminHandle || 'admin').trim().replace(/^@/, '').toLowerCase()
        const displayName = (body.displayName || businessName).trim()
        const email = (body.email || `${adminHandle}@chatze.np`).trim()
        const password = (body.password || 'nepal123').trim()

        if (env.DB) {
          const userId = `usr_${crypto.randomUUID().slice(0, 8)}`
          await env.DB.prepare(
            'INSERT OR IGNORE INTO "user" (id, name, email, passwordHash, emailVerified) VALUES (?, ?, ?, ?, 1)'
          ).bind(userId, displayName, email, password).run()

          await env.DB.prepare(
            'INSERT OR REPLACE INTO profiles (userId, username, displayName, role) VALUES (?, ?, ?, ?)'
          ).bind(userId, adminHandle, displayName, 'admin').run()

          await env.DB.prepare(
            "INSERT OR REPLACE INTO system_config (key, value) VALUES ('instance_name', ?)"
          ).bind(businessName).run()

          // Generate ECDSA P-256 keypair
          try {
            const keyPair = await crypto.subtle.generateKey(
              { name: 'ECDSA', namedCurve: 'P-256' },
              true,
              ['sign', 'verify']
            )
            const spki = await crypto.subtle.exportKey('spki', keyPair.publicKey)
            const pubB64 = btoa(String.fromCharCode(...new Uint8Array(spki)))
            const jwk = await crypto.subtle.exportKey('jwk', keyPair.privateKey)
            await env.DB.prepare(
              "INSERT OR REPLACE INTO system_config (key, value) VALUES ('federation_public_key', ?)"
            ).bind(pubB64).run()
            await env.DB.prepare(
              "INSERT OR REPLACE INTO system_config (key, value) VALUES ('federation_private_key_jwk', ?)"
            ).bind(JSON.stringify(jwk)).run()
          } catch (e) {
            console.warn('[ECDSA Keygen]', e)
          }

          // Concierge Welcome message
          const guideId = 'chatze_guide'
          const convId = `conv_welcome_${crypto.randomUUID().slice(0, 8)}`
          await env.DB.prepare(
            'INSERT OR IGNORE INTO profiles (userId, username, displayName, role) VALUES (?, ?, ?, ?)'
          ).bind(guideId, guideId, 'Chatze Nepal Concierge', 'bot').run()

          const [uA, uB] = [userId, guideId].sort()
          await env.DB.prepare(
            'INSERT OR IGNORE INTO conversations (id, userAId, userBId, status, lastMessageSnippet) VALUES (?, ?, ?, ?, ?)'
          ).bind(convId, uA, uB, 'active', '🙏 Namaste! Welcome to Chatze Nepal Edition.').run()

          await env.DB.prepare(
            'INSERT INTO messages (id, conversationId, senderId, body) VALUES (?, ?, ?, ?)'
          ).bind(
            `msg_${crypto.randomUUID().slice(0, 8)}`,
            convId,
            guideId,
            `🙏 Namaste and welcome to Chatze Nepal Edition!\n\nYour 100% Free-Forever Cloudflare Edge inbox is active on the Kathmandu (KTM) PoP:\n• 5 GB Free Cloudflare D1 Native Database (~25M messages)\n• 100,000 requests/day\n• Share your store link /u/${adminHandle} on Facebook, TikTok, or Instagram.\n• Use Nepal Quick Canned Responses to answer customers in 1 second!`
          ).run()

          // Create session
          const sessionToken = `sess_${crypto.randomUUID().replace(/-/g, '')}`
          const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString()
          await env.DB.prepare(
            'INSERT INTO session (id, expiresAt, token, userId) VALUES (?, ?, ?, ?)'
          ).bind(`s_${crypto.randomUUID().slice(0, 8)}`, expiresAt, sessionToken, userId).run()

          return new Response(JSON.stringify({
            ok: true,
            message: 'Workspace initialized successfully',
            adminHandle,
            businessName,
            user: { id: userId, username: adminHandle, displayName, role: 'admin' },
            sessionToken
          }), {
            headers: {
              'Content-Type': 'application/json',
              'Set-Cookie': `chatze_session=${sessionToken}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`
            }
          })
        }

        return new Response(JSON.stringify({ ok: false, error: 'Database binding (DB) not ready' }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' }
        })
      }
    }

    // 4. Authentication Endpoints (/api/auth/sign-in, /api/auth/sign-up, /api/auth/me, /api/auth/sign-out)
    if (pathname === '/api/auth/me') {
      const user = await getUserFromRequest(request, env)
      return new Response(JSON.stringify({ user }), {
        headers: { 'Content-Type': 'application/json' }
      })
    }

    if (pathname === '/api/auth/sign-in' && request.method === 'POST') {
      const body = await request.json().catch(() => ({}))
      const username = (body.username || '').trim().replace(/^@/, '').toLowerCase()
      const password = (body.password || '').trim()

      if (!env.DB) {
        return new Response(JSON.stringify({ ok: false, error: 'Database not ready' }), { status: 500 })
      }

      const profile = await env.DB.prepare(
        'SELECT p.userId, p.username, p.displayName, p.role, u.passwordHash, u.email FROM profiles p JOIN "user" u ON p.userId = u.id WHERE LOWER(p.username) = ? OR LOWER(u.email) = ?'
      ).bind(username, username).first()

      if (!profile) {
        return new Response(JSON.stringify({ ok: false, error: 'User not found. Please sign up or initialize setup.' }), { status: 401 })
      }

      if (profile.passwordHash && profile.passwordHash !== password) {
        return new Response(JSON.stringify({ ok: false, error: 'Incorrect password.' }), { status: 401 })
      }

      const sessionToken = `sess_${crypto.randomUUID().replace(/-/g, '')}`
      const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString()
      await env.DB.prepare(
        'INSERT INTO session (id, expiresAt, token, userId) VALUES (?, ?, ?, ?)'
      ).bind(`s_${crypto.randomUUID().slice(0, 8)}`, expiresAt, sessionToken, profile.userId).run()

      return new Response(JSON.stringify({
        ok: true,
        user: { id: profile.userId, username: profile.username, displayName: profile.displayName, role: profile.role },
        sessionToken
      }), {
        headers: {
          'Content-Type': 'application/json',
          'Set-Cookie': `chatze_session=${sessionToken}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`
        }
      })
    }

    if (pathname === '/api/auth/sign-up' && request.method === 'POST') {
      const body = await request.json().catch(() => ({}))
      const username = (body.username || '').trim().replace(/^@/, '').toLowerCase()
      const displayName = (body.displayName || username).trim()
      const password = (body.password || '').trim()
      const email = (body.email || `${username}@chatze.np`).trim().toLowerCase()

      if (!username) {
        return new Response(JSON.stringify({ ok: false, error: 'Username is required' }), { status: 400 })
      }

      if (!env.DB) {
        return new Response(JSON.stringify({ ok: false, error: 'Database not ready' }), { status: 500 })
      }

      // Check if username taken
      const existing = await env.DB.prepare('SELECT userId FROM profiles WHERE LOWER(username) = ?').bind(username).first()
      if (existing) {
        return new Response(JSON.stringify({ ok: false, error: 'Username already taken' }), { status: 400 })
      }

      const userId = `usr_${crypto.randomUUID().slice(0, 8)}`
      await env.DB.prepare(
        'INSERT INTO "user" (id, name, email, passwordHash, emailVerified) VALUES (?, ?, ?, ?, 1)'
      ).bind(userId, displayName, email, password).run()

      await env.DB.prepare(
        'INSERT INTO profiles (userId, username, displayName, role) VALUES (?, ?, ?, ?)'
      ).bind(userId, username, displayName, 'customer').run()

      // Automatically create conversation with Admin
      const admin = await env.DB.prepare("SELECT userId FROM profiles WHERE role = 'admin' LIMIT 1").first()
      if (admin && admin.userId !== userId) {
        const [uA, uB] = [userId, admin.userId].sort()
        const convId = `conv_${crypto.randomUUID().slice(0, 8)}`
        await env.DB.prepare(
          'INSERT OR IGNORE INTO conversations (id, userAId, userBId, status, lastMessageSnippet) VALUES (?, ?, ?, ?, ?)'
        ).bind(convId, uA, uB, 'active', '👋 New customer conversation started').run()
      }

      const sessionToken = `sess_${crypto.randomUUID().replace(/-/g, '')}`
      const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString()
      await env.DB.prepare(
        'INSERT INTO session (id, expiresAt, token, userId) VALUES (?, ?, ?, ?)'
      ).bind(`s_${crypto.randomUUID().slice(0, 8)}`, expiresAt, sessionToken, userId).run()

      return new Response(JSON.stringify({
        ok: true,
        user: { id: userId, username, displayName, role: 'customer' },
        sessionToken
      }), {
        headers: {
          'Content-Type': 'application/json',
          'Set-Cookie': `chatze_session=${sessionToken}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`
        }
      })
    }

    if (pathname === '/api/auth/sign-out' && request.method === 'POST') {
      return new Response(JSON.stringify({ ok: true }), {
        headers: {
          'Content-Type': 'application/json',
          'Set-Cookie': 'chatze_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0'
        }
      })
    }

    // 5. Messaging & Inbox APIs (/api/conversations, /api/messages, /api/messaging)
    if (pathname === '/api/conversations' && request.method === 'GET') {
      const user = await getUserFromRequest(request, env)
      const currentUserId = user?.userId || url.searchParams.get('userId')
      if (!currentUserId || !env.DB) {
        return new Response(JSON.stringify({ conversations: [] }), { headers: { 'Content-Type': 'application/json' } })
      }

      const rows = await env.DB.prepare(
        `SELECT c.id, c.userAId, c.userBId, c.status, c.lastMessageSnippet, c.lastMessageAt, c.createdAt,
                pA.displayName as userAName, pA.username as userAHandle,
                pB.displayName as userBName, pB.username as userBHandle
         FROM conversations c
         LEFT JOIN profiles pA ON c.userAId = pA.userId
         LEFT JOIN profiles pB ON c.userBId = pB.userId
         WHERE c.userAId = ? OR c.userBId = ?
         ORDER BY c.createdAt DESC LIMIT 100`
      ).bind(currentUserId, currentUserId).all()

      const list = (rows.results || []).map((r) => {
        const isA = r.userAId === currentUserId
        return {
          id: r.id,
          otherUserId: isA ? r.userBId : r.userAId,
          otherName: isA ? r.userBName : r.userAName,
          otherHandle: isA ? r.userBHandle : r.userAHandle,
          lastSnippet: r.lastMessageSnippet || 'No messages yet',
          status: r.status,
          updatedAt: r.lastMessageAt || r.createdAt
        }
      })

      return new Response(JSON.stringify({ conversations: list }), {
        headers: { 'Content-Type': 'application/json' }
      })
    }

    if (pathname === '/api/messages' && request.method === 'GET') {
      const conversationId = url.searchParams.get('conversationId')
      if (!conversationId || !env.DB) {
        return new Response(JSON.stringify({ messages: [] }), { headers: { 'Content-Type': 'application/json' } })
      }

      const rows = await env.DB.prepare(
        'SELECT m.*, p.displayName as senderName, p.username as senderHandle FROM messages m LEFT JOIN profiles p ON m.senderId = p.userId WHERE m.conversationId = ? ORDER BY m.createdAt ASC LIMIT 150'
      ).bind(conversationId).all()

      return new Response(JSON.stringify({ messages: rows.results || [] }), {
        headers: { 'Content-Type': 'application/json' }
      })
    }

    if (pathname === '/api/messaging' && request.method === 'POST') {
      const user = await getUserFromRequest(request, env)
      const body = await request.json().catch(() => ({}))
      const conversationId = body.conversationId
      const text = (body.body || '').trim()
      const senderId = user?.userId || body.senderId

      if (!conversationId || !text || !senderId) {
        return new Response(JSON.stringify({ error: 'Missing conversationId, body or senderId' }), { status: 400 })
      }

      if (env.DB) {
        const msgId = `msg_${crypto.randomUUID().slice(0, 8)}`
        const now = new Date().toISOString()
        await env.DB.prepare(
          'INSERT INTO messages (id, conversationId, senderId, body, createdAt) VALUES (?, ?, ?, ?, ?)'
        ).bind(msgId, conversationId, senderId, text, now).run()

        await env.DB.prepare(
          'UPDATE conversations SET lastMessageSnippet = ?, lastMessageAt = ? WHERE id = ?'
        ).bind(text.slice(0, 100), now, conversationId).run()

        const msgObj = { id: msgId, conversationId, senderId, body: text, createdAt: now }

        // Find recipient to dispatch SSE event
        const conv = await env.DB.prepare('SELECT userAId, userBId FROM conversations WHERE id = ?').bind(conversationId).first()
        if (conv) {
          const recipientId = conv.userAId === senderId ? conv.userBId : conv.userAId
          broadcastUserEvent(recipientId, { type: 'message', message: msgObj, conversationId })
          broadcastUserEvent(senderId, { type: 'message', message: msgObj, conversationId })
        }

        return new Response(JSON.stringify({ ok: true, message: msgObj }), {
          headers: { 'Content-Type': 'application/json' }
        })
      }

      return new Response(JSON.stringify({ error: 'D1 DB not bound' }), { status: 500 })
    }

    // 6. Asymmetric Federation Discovery (/api/federation/identity)
    if (pathname === '/api/federation/identity') {
      let pubKey = ''
      let instanceName = 'Chatze Nepal Edition'
      if (env.DB) {
        try {
          const row = await env.DB.prepare("SELECT value FROM system_config WHERE key = 'federation_public_key'").first()
          if (row?.value) pubKey = row.value
          const nameRow = await env.DB.prepare("SELECT value FROM system_config WHERE key = 'instance_name'").first()
          if (nameRow?.value) instanceName = nameRow.value
        } catch {}
      }
      return new Response(JSON.stringify({
        instance_url: url.origin,
        name: instanceName,
        public_key: pubKey,
        algorithm: 'ECDSA-P256-SHA256',
        region: 'KTM',
        created_at: Math.floor(Date.now() / 1000)
      }), {
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Cache-Control': 'public, max-age=86400'
        }
      })
    }

    // 7. Automated 90-Day Retention Endpoint (/api/cron/retention)
    if (pathname === '/api/cron/retention') {
      let purged = 0
      if (env.DB) {
        try {
          const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString()
          const res = await env.DB.prepare(
            `DELETE FROM messages WHERE createdAt < ? AND conversationId IN 
             (SELECT id FROM conversations WHERE status = 'archived')`
          ).bind(ninetyDaysAgo).run()
          purged = res?.meta?.changes || 0
        } catch (e) {
          console.warn('[Retention Cleanup]', e)
        }
      }
      return new Response(JSON.stringify({
        ok: true,
        purgedMessagesCount: purged,
        storageEngine: 'Cloudflare D1 Native Database',
        timestamp: new Date().toISOString()
      }), {
        headers: { 'Content-Type': 'application/json' }
      })
    }

    // 8. Serve Static Assets if present (CSS, JS, Icons)
    if (env.ASSETS) {
      try {
        const assetResponse = await env.ASSETS.fetch(request)
        if (assetResponse.status === 200) {
          return assetResponse
        }
      } catch {}
    }

    // 9. Primary Application UI (HTML/SPA)
    // Renders the First-Launch Setup Wizard, Login/Signup, or Main Real-time Chat
    return new Response(renderChatzeAppHtml(), {
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-cache'
      }
    })
  },

  // 10. Nightly Scheduled Cron Trigger (crons = ["0 3 * * *"])
  async scheduled(event, env, ctx) {
    if (!env.DB) return
    ctx.waitUntil((async () => {
      try {
        const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString()
        await env.DB.prepare(
          `DELETE FROM messages WHERE createdAt < ? AND conversationId IN 
           (SELECT id FROM conversations WHERE status = 'archived')`
        ).bind(ninetyDaysAgo).run()
        console.log('[Cloudflare Cron] Nightly 90-day retention purge completed.')
      } catch (err) {
        console.warn('[Cloudflare Cron Error]', err)
      }
    })())
  }
}

function renderChatzeAppHtml() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>Chatze Nepal Edition - High-Volume Edge Inbox</title>
  <meta name="description" content="100% Free-Forever, Zero-Setup Cloudflare Edge Messaging for High-Volume Nepal Businesses">
  <link rel="icon" href="/icon.svg" type="image/svg+xml">
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');
    body { font-family: 'Plus Jakarta Sans', sans-serif; }
    .scrollbar-thin::-webkit-scrollbar { width: 5px; }
    .scrollbar-thin::-webkit-scrollbar-track { background: transparent; }
    .scrollbar-thin::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 4px; }
    .scrollbar-thin::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
  </style>
</head>
<body class="bg-slate-900 text-slate-100 min-h-screen flex flex-col antialiased select-none">

  <!-- TOP HEADER / KTM EDGE BAR -->
  <header class="bg-slate-950/80 backdrop-blur border-b border-slate-800 px-4 py-2.5 flex items-center justify-between sticky top-0 z-40">
    <div class="flex items-center gap-3">
      <div class="h-8 w-8 rounded-lg bg-emerald-600 flex items-center justify-center font-black text-white shadow-md shadow-emerald-900/50">
        <i class="fa-solid fa-paper-plane text-sm"></i>
      </div>
      <div>
        <div class="flex items-center gap-2">
          <span class="font-extrabold text-white text-base tracking-tight" id="headerTitle">Chatze Nepal</span>
          <span class="bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-[10px] font-semibold px-2 py-0.5 rounded-full flex items-center gap-1">
            <span class="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse"></span> KTM Edge Active
          </span>
        </div>
        <div class="text-[11px] text-slate-400 flex items-center gap-2">
          <span id="latencyBadge"><i class="fa-solid fa-bolt text-amber-400"></i> Kathmandu Ping: 12ms</span>
          <span>•</span>
          <span class="text-slate-400">D1: 5 GB Free</span>
        </div>
      </div>
    </div>

    <!-- Header Actions -->
    <div class="flex items-center gap-2" id="headerUserActions">
      <!-- Injected dynamically based on auth state -->
    </div>
  </header>

  <!-- MAIN APP CONTAINER -->
  <main class="flex-1 flex flex-col relative overflow-hidden" id="appRoot">
    <!-- Initial Loading State -->
    <div class="flex-1 flex flex-col items-center justify-center p-6 text-center" id="loadingView">
      <div class="h-12 w-12 border-4 border-emerald-500/20 border-t-emerald-500 rounded-full animate-spin mb-4"></div>
      <h2 class="text-lg font-bold text-white">Connecting to Kathmandu Edge...</h2>
      <p class="text-xs text-slate-400 mt-1">Checking Cloudflare D1 Native Database</p>
    </div>
  </main>

  <script>
    // Global State
    let currentUser = null;
    let setupInfo = null;
    let conversations = [];
    let activeConversation = null;
    let messages = [];
    let sseEventSource = null;

    async function initApp() {
      try {
        // Measure Kathmandu ping
        const t0 = performance.now();
        const healthRes = await fetch('/api/health');
        const ping = Math.round(performance.now() - t0);
        document.getElementById('latencyBadge').innerHTML = '<i class="fa-solid fa-bolt text-emerald-400"></i> KTM Ping: ' + ping + 'ms';

        // Check Setup State
        const setupRes = await fetch('/api/setup');
        setupInfo = await setupRes.json();

        if (setupInfo.instanceName) {
          document.getElementById('headerTitle').textContent = setupInfo.instanceName;
        }

        if (!setupInfo.initialized) {
          renderSetupWizard();
          return;
        }

        // Check Session
        const authRes = await fetch('/api/auth/me');
        const authData = await authRes.json();
        currentUser = authData.user;

        if (!currentUser) {
          renderAuthView('signin');
          return;
        }

        renderChatWorkspace();
      } catch (err) {
        console.error('Init error:', err);
        renderErrorView(err.message);
      }
    }

    // 1. SETUP WIZARD (First Boot)
    function renderSetupWizard() {
      const root = document.getElementById('appRoot');
      document.getElementById('headerUserActions').innerHTML = '';
      root.innerHTML = \`
        <div class="flex-1 flex items-center justify-center p-4 bg-gradient-to-b from-slate-900 via-slate-900 to-slate-950">
          <div class="w-full max-w-md bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-2xl shadow-emerald-950/20">
            <div class="text-center mb-6">
              <div class="inline-flex p-3 rounded-2xl bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 mb-3">
                <i class="fa-solid fa-store text-2xl"></i>
              </div>
              <h2 class="text-xl font-black text-white">First-Launch Store Setup</h2>
              <p class="text-xs text-slate-400 mt-1">Set up your business admin account in 60 seconds. Zero config required.</p>
            </div>

            <form id="setupForm" class="space-y-4">
              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Business / Store Name</label>
                <input type="text" id="setupBizName" required placeholder="e.g. New Road Electronics" class="w-full bg-slate-800 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
              </div>

              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Admin Handle</label>
                <div class="flex rounded-xl bg-slate-800 border border-slate-700 overflow-hidden focus-within:border-emerald-500">
                  <span class="px-3 py-2.5 text-slate-500 text-sm font-bold bg-slate-800/80">@</span>
                  <input type="text" id="setupAdminHandle" required value="admin" placeholder="admin" class="flex-1 bg-transparent px-2 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none">
                </div>
                <p class="text-[11px] text-slate-500 mt-1">Customers will reach you at: /u/<span id="handlePreview">admin</span></p>
              </div>

              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Admin Password</label>
                <input type="password" id="setupAdminPass" required placeholder="••••••••••••" class="w-full bg-slate-800 border border-slate-700 rounded-xl px-3.5 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
              </div>

              <div class="p-3 bg-emerald-950/30 border border-emerald-800/40 rounded-xl text-[11px] text-emerald-300 flex items-start gap-2">
                <i class="fa-solid fa-shield-halved mt-0.5 text-emerald-400"></i>
                <span>Automatically initializes your Cloudflare D1 tables and generates asymmetric ECDSA cryptographic federation keys.</span>
              </div>

              <button type="submit" id="setupSubmitBtn" class="w-full py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl text-sm transition-all shadow-lg shadow-emerald-900/30 flex items-center justify-center gap-2 cursor-pointer">
                <span>Launch Business Workspace</span>
                <i class="fa-solid fa-arrow-right text-xs"></i>
              </button>
            </form>
          </div>
        </div>
      \`;

      const handleInput = document.getElementById('setupAdminHandle');
      const handlePreview = document.getElementById('handlePreview');
      handleInput.addEventListener('input', () => {
        handlePreview.textContent = handleInput.value.trim().toLowerCase().replace(/^@/, '') || 'admin';
      });

      document.getElementById('setupForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const btn = document.getElementById('setupSubmitBtn');
        btn.disabled = true;
        btn.innerHTML = '<i class="fa-solid fa-circle-notch animate-spin"></i> Initializing D1 Database...';

        try {
          const res = await fetch('/api/setup', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              businessName: document.getElementById('setupBizName').value,
              adminHandle: document.getElementById('setupAdminHandle').value,
              password: document.getElementById('setupAdminPass').value
            })
          });
          const data = await res.json();
          if (data.ok) {
            currentUser = data.user;
            initApp();
          } else {
            alert(data.error || 'Initialization failed');
            btn.disabled = false;
            btn.innerHTML = 'Try Again';
          }
        } catch (err) {
          alert('Error: ' + err.message);
          btn.disabled = false;
          btn.innerHTML = 'Try Again';
        }
      });
    }

    // 2. AUTH VIEW (Sign In / Sign Up)
    function renderAuthView(tab = 'signin') {
      const root = document.getElementById('appRoot');
      document.getElementById('headerUserActions').innerHTML = '';
      root.innerHTML = \`
        <div class="flex-1 flex items-center justify-center p-4 bg-gradient-to-b from-slate-900 via-slate-900 to-slate-950">
          <div class="w-full max-w-sm bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-2xl">
            <!-- Tabs -->
            <div class="flex bg-slate-800 p-1 rounded-xl mb-6">
              <button onclick="renderAuthView('signin')" class="flex-1 py-1.5 text-xs font-bold rounded-lg transition-all \${tab === 'signin' ? 'bg-slate-700 text-white shadow' : 'text-slate-400 hover:text-white'}">Sign In</button>
              <button onclick="renderAuthView('signup')" class="flex-1 py-1.5 text-xs font-bold rounded-lg transition-all \${tab === 'signup' ? 'bg-slate-700 text-white shadow' : 'text-slate-400 hover:text-white'}">New Customer / Register</button>
            </div>

            <div class="text-center mb-5">
              <h2 class="text-lg font-black text-white">\${tab === 'signin' ? 'Welcome Back' : 'Create Customer Account'}</h2>
              <p class="text-xs text-slate-400 mt-1">\${tab === 'signin' ? 'Log in with your admin or customer handle' : 'Start chatting with local stores in Nepal'}</p>
            </div>

            <form id="authForm" class="space-y-4">
              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Handle or Username</label>
                <div class="flex rounded-xl bg-slate-800 border border-slate-700 overflow-hidden focus-within:border-emerald-500">
                  <span class="px-3 py-2 text-slate-500 text-sm font-bold bg-slate-800/80">@</span>
                  <input type="text" id="authUsername" required placeholder="admin" class="flex-1 bg-transparent px-2 py-2 text-sm text-white placeholder-slate-500 focus:outline-none">
                </div>
              </div>

              \${tab === 'signup' ? \`
              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Full Name</label>
                <input type="text" id="authDisplayName" required placeholder="Aayush Sharma" class="w-full bg-slate-800 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
              </div>
              \` : ''}

              <div>
                <label class="block text-xs font-semibold text-slate-300 mb-1">Password</label>
                <input type="password" id="authPassword" required placeholder="••••••••••••" class="w-full bg-slate-800 border border-slate-700 rounded-xl px-3 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
              </div>

              <button type="submit" id="authSubmitBtn" class="w-full py-2.5 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl text-sm transition-all shadow-lg shadow-emerald-900/30 flex items-center justify-center gap-2 cursor-pointer mt-2">
                <span>\${tab === 'signin' ? 'Sign In to Chatze' : 'Create Account & Enter'}</span>
                <i class="fa-solid fa-arrow-right text-xs"></i>
              </button>
            </form>

            <div class="mt-4 pt-4 border-t border-slate-800 text-center">
              <span class="text-[11px] text-slate-500">Nepal PoP: KTM Edge • Cloudflare D1 Native</span>
            </div>
          </div>
        </div>
      \`;

      document.getElementById('authForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const btn = document.getElementById('authSubmitBtn');
        btn.disabled = true;
        btn.innerHTML = '<i class="fa-solid fa-circle-notch animate-spin"></i> Processing...';

        const endpoint = tab === 'signin' ? '/api/auth/sign-in' : '/api/auth/sign-up';
        const payload = {
          username: document.getElementById('authUsername').value,
          password: document.getElementById('authPassword').value
        };
        if (tab === 'signup') {
          payload.displayName = document.getElementById('authDisplayName').value;
        }

        try {
          const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });
          const data = await res.json();
          if (data.ok) {
            currentUser = data.user;
            initApp();
          } else {
            alert(data.error || 'Authentication error');
            btn.disabled = false;
            btn.innerHTML = tab === 'signin' ? 'Sign In' : 'Create Account';
          }
        } catch (err) {
          alert('Error: ' + err.message);
          btn.disabled = false;
          btn.innerHTML = 'Try Again';
        }
      });
    }

    // 3. MAIN CHAT WORKSPACE
    async function renderChatWorkspace() {
      // Setup header user info
      document.getElementById('headerUserActions').innerHTML = \`
        <div class="flex items-center gap-2 bg-slate-800/80 px-2.5 py-1 rounded-xl border border-slate-700 text-xs">
          <span class="h-2 w-2 rounded-full bg-emerald-400"></span>
          <span class="font-bold text-white">@\${currentUser.username}</span>
          <span class="text-[10px] text-slate-400 bg-slate-700/50 px-1.5 py-0.5 rounded uppercase font-semibold">\${currentUser.role}</span>
        </div>
        <button onclick="handleSignOut()" class="p-1.5 text-slate-400 hover:text-rose-400 transition-colors text-xs" title="Sign Out">
          <i class="fa-solid fa-arrow-right-from-bracket"></i>
        </button>
      \`;

      const root = document.getElementById('appRoot');
      root.innerHTML = \`
        <div class="flex-1 flex overflow-hidden">
          <!-- LEFT SIDEBAR: CONVERSATIONS QUEUE -->
          <aside class="w-80 border-r border-slate-800 bg-slate-950 flex flex-col shrink-0">
            <!-- Search & Actions -->
            <div class="p-3 border-b border-slate-800/80 space-y-2">
              <div class="relative">
                <i class="fa-solid fa-magnifying-glass absolute left-3 top-2.5 text-slate-500 text-xs"></i>
                <input type="text" id="searchConvInput" placeholder="Search customer or handle..." class="w-full bg-slate-900 border border-slate-800 rounded-xl pl-8 pr-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
              </div>

              <!-- Store Link Pill -->
              <div class="p-2 bg-emerald-950/20 border border-emerald-800/30 rounded-xl flex items-center justify-between text-[11px]">
                <div class="truncate text-emerald-400">
                  <i class="fa-solid fa-share-nodes mr-1 text-emerald-500"></i> Store link: <b>/u/\${currentUser.username}</b>
                </div>
                <button onclick="copyStoreLink()" class="px-2 py-0.5 bg-emerald-600/40 hover:bg-emerald-600/60 text-emerald-200 text-[10px] font-bold rounded transition-colors cursor-pointer">
                  Copy
                </button>
              </div>
            </div>

            <!-- Conversation List -->
            <div class="flex-1 overflow-y-auto scrollbar-thin p-2 space-y-1" id="convListContainer">
              <div class="p-4 text-center text-xs text-slate-500">Loading inbox...</div>
            </div>
          </aside>

          <!-- RIGHT PANE: ACTIVE CHAT -->
          <section class="flex-1 flex flex-col bg-slate-900 overflow-hidden" id="chatPane">
            <div class="flex-1 flex flex-col items-center justify-center p-6 text-center text-slate-400">
              <i class="fa-regular fa-comments text-4xl mb-3 text-slate-600"></i>
              <h3 class="text-base font-bold text-white">Select a customer conversation</h3>
              <p class="text-xs text-slate-500 max-w-sm mt-1">High-volume incoming inquiries from TikTok, Facebook, and Instagram land here with 100s SSE streaming.</p>
            </div>
          </section>
        </div>
      \`;

      await loadConversations();
      initSSE();
    }

    // Load Conversations
    async function loadConversations() {
      try {
        const res = await fetch('/api/conversations');
        const data = await res.json();
        conversations = data.conversations || [];
        renderConversationList();

        if (conversations.length > 0 && !activeConversation) {
          selectConversation(conversations[0]);
        }
      } catch (err) {
        console.error('Error loading conversations:', err);
      }
    }

    function renderConversationList() {
      const container = document.getElementById('convListContainer');
      if (!container) return;

      if (conversations.length === 0) {
        container.innerHTML = \`
          <div class="p-6 text-center text-slate-500">
            <i class="fa-solid fa-inbox text-2xl mb-2 text-slate-600"></i>
            <p class="text-xs">No active conversations yet.</p>
            <p class="text-[10px] text-slate-600 mt-1">Share your link to receive inquiries.</p>
          </div>
        \`;
        return;
      }

      container.innerHTML = conversations.map(c => {
        const isActive = activeConversation && activeConversation.id === c.id;
        return \`
          <div onclick="selectConversationById('\${c.id}')" class="p-3 rounded-xl cursor-pointer transition-all flex items-start gap-3 \${isActive ? 'bg-slate-800 border border-slate-700/80 shadow' : 'hover:bg-slate-900 border border-transparent'}">
            <div class="h-9 w-9 rounded-xl bg-slate-800 border border-slate-700 flex items-center justify-center font-bold text-xs text-emerald-400 shrink-0">
              \${(c.otherName || c.otherHandle || 'U').charAt(0).toUpperCase()}
            </div>
            <div class="flex-1 min-w-0">
              <div class="flex items-center justify-between">
                <span class="font-bold text-xs text-white truncate">\${c.otherName || c.otherHandle}</span>
                <span class="text-[10px] text-slate-500">Live</span>
              </div>
              <p class="text-[11px] text-slate-400 truncate mt-0.5">\${c.lastSnippet}</p>
            </div>
          </div>
        \`;
      }).join('');
    }

    window.selectConversationById = function(id) {
      const conv = conversations.find(c => c.id === id);
      if (conv) selectConversation(conv);
    }

    async function selectConversation(conv) {
      activeConversation = conv;
      renderConversationList();

      const pane = document.getElementById('chatPane');
      pane.innerHTML = \`
        <!-- Conversation Header -->
        <div class="px-4 py-3 bg-slate-950/60 border-b border-slate-800 flex items-center justify-between">
          <div class="flex items-center gap-3">
            <div class="h-9 w-9 rounded-xl bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 flex items-center justify-center font-bold text-sm">
              \${(conv.otherName || conv.otherHandle || 'C').charAt(0).toUpperCase()}
            </div>
            <div>
              <div class="flex items-center gap-2">
                <span class="font-bold text-sm text-white">\${conv.otherName}</span>
                <span class="text-xs text-slate-400">@\${conv.otherHandle}</span>
              </div>
              <div class="text-[11px] text-emerald-400 flex items-center gap-1.5">
                <span class="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse"></span>
                <span>Active via Cloudflare Kathmandu Edge</span>
              </div>
            </div>
          </div>

          <div class="flex items-center gap-2">
            <span class="text-[10px] bg-slate-800 border border-slate-700 text-slate-400 px-2 py-1 rounded-lg">
              <i class="fa-solid fa-database text-emerald-400 mr-1"></i> D1 SQLite
            </span>
          </div>
        </div>

        <!-- Messages Feed -->
        <div class="flex-1 overflow-y-auto scrollbar-thin p-4 space-y-3" id="messagesFeed">
          <div class="text-center text-xs text-slate-500 my-4">Loading messages...</div>
        </div>

        <!-- Nepal Quick Canned Responses -->
        <div class="px-4 py-2 bg-slate-950/40 border-t border-slate-800/80 flex items-center gap-1.5 overflow-x-auto scrollbar-thin text-xs">
          <span class="text-[11px] font-semibold text-slate-400 shrink-0 mr-1">Quick:</span>
          <button onclick="insertQuickResponse('🙏 Namaste! Hajur, product available chha.')" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-[11px] whitespace-nowrap transition-colors">
            🙏 Namaste, stock available!
          </button>
          <button onclick="insertQuickResponse('🚚 Inside Kathmandu Valley 24 hours ma delivery hunchha.')" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-[11px] whitespace-nowrap transition-colors">
            🚚 KTM 24hr delivery
          </button>
          <button onclick="insertQuickResponse('💰 Payment: Cash on Delivery / eSewa / Khalti available.')" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-[11px] whitespace-nowrap transition-colors">
            💰 COD / eSewa / Khalti
          </button>
          <button onclick="insertQuickResponse('📍 Store Location: New Road, Kathmandu. Timings: 10AM - 7PM.')" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-lg text-[11px] whitespace-nowrap transition-colors">
            📍 New Road location
          </button>
        </div>

        <!-- Message Input -->
        <div class="p-3 bg-slate-950 border-t border-slate-800">
          <form id="msgForm" class="flex items-center gap-2">
            <input type="text" id="msgInput" required placeholder="Type reply in Nepali or English..." class="flex-1 bg-slate-900 border border-slate-800 rounded-xl px-4 py-2.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-emerald-500">
            <button type="submit" id="msgSendBtn" class="px-4 py-2.5 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-xl text-sm transition-all shadow-md shadow-emerald-900/30 flex items-center gap-2 cursor-pointer">
              <span>Send</span>
              <i class="fa-solid fa-paper-plane text-xs"></i>
            </button>
          </form>
        </div>
      \`;

      document.getElementById('msgForm').addEventListener('submit', async (e) => {
        e.preventDefault();
        const input = document.getElementById('msgInput');
        const text = input.value.trim();
        if (!text) return;
        input.value = '';

        try {
          const res = await fetch('/api/messaging', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              conversationId: activeConversation.id,
              body: text,
              senderId: currentUser.id
            })
          });
          const data = await res.json();
          if (data.ok) {
            messages.push(data.message);
            renderMessages();
            loadConversations();
          }
        } catch (err) {
          console.error('Send error:', err);
        }
      });

      await loadMessages(conv.id);
    }

    window.insertQuickResponse = function(text) {
      const input = document.getElementById('msgInput');
      if (input) {
        input.value = text;
        input.focus();
      }
    }

    async function loadMessages(convId) {
      try {
        const res = await fetch('/api/messages?conversationId=' + convId);
        const data = await res.json();
        messages = data.messages || [];
        renderMessages();
      } catch (err) {
        console.error('Error loading messages:', err);
      }
    }

    function renderMessages() {
      const feed = document.getElementById('messagesFeed');
      if (!feed) return;

      if (messages.length === 0) {
        feed.innerHTML = '<div class="text-center text-xs text-slate-500 my-8">No messages in this inquiry yet. Say namaste!</div>';
        return;
      }

      feed.innerHTML = messages.map(m => {
        const isMine = m.senderId === currentUser.id;
        return \`
          <div class="flex flex-col \${isMine ? 'items-end' : 'items-start'}">
            <div class="max-w-[75%] rounded-2xl px-4 py-2.5 text-xs \${isMine ? 'bg-emerald-600 text-white rounded-br-none shadow-md shadow-emerald-950/20' : 'bg-slate-800 text-slate-100 rounded-bl-none border border-slate-700/80'}">
              <p class="whitespace-pre-wrap leading-relaxed">\${escapeHtml(m.body)}</p>
            </div>
            <span class="text-[10px] text-slate-500 mt-1 px-1">\${formatTime(m.createdAt)}</span>
          </div>
        \`;
      }).join('');

      feed.scrollTop = feed.scrollHeight;
    }

    // Real-Time SSE Listener
    function initSSE() {
      if (sseEventSource) sseEventSource.close();
      const sseUrl = '/api/stream?userId=' + currentUser.id;
      sseEventSource = new EventSource(sseUrl);

      sseEventSource.addEventListener('event', (e) => {
        try {
          const payload = JSON.parse(e.data);
          if (payload.type === 'message') {
            if (activeConversation && activeConversation.id === payload.conversationId) {
              messages.push(payload.message);
              renderMessages();
            }
            loadConversations();
          }
        } catch {}
      });

      sseEventSource.addEventListener('cycle', () => {
        sseEventSource.close();
        setTimeout(initSSE, 500);
      });

      sseEventSource.onerror = () => {
        sseEventSource.close();
        setTimeout(initSSE, 3000);
      };
    }

    window.copyStoreLink = function() {
      const url = window.location.origin + '/u/' + currentUser.username;
      navigator.clipboard.writeText(url);
      alert('Copied your business store link:\\n' + url + '\\n\\nShare on Facebook / TikTok for customers to message you!');
    }

    window.handleSignOut = async function() {
      if (confirm('Sign out of Chatze?')) {
        await fetch('/api/auth/sign-out', { method: 'POST' });
        window.location.reload();
      }
    }

    function escapeHtml(str) {
      if (!str) return '';
      return str.replace(/[&<>"']/g, m => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;'
      }[m]));
    }

    function formatTime(isoStr) {
      if (!isoStr) return '';
      try {
        const d = new Date(isoStr);
        return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      } catch {
        return '';
      }
    }

    function renderErrorView(msg) {
      document.getElementById('appRoot').innerHTML = \`
        <div class="flex-1 flex flex-col items-center justify-center p-6 text-center">
          <i class="fa-solid fa-triangle-exclamation text-rose-500 text-3xl mb-3"></i>
          <h2 class="text-base font-bold text-white">Cloudflare Edge Connection Error</h2>
          <p class="text-xs text-slate-400 mt-1 max-w-md">\${msg}</p>
          <button onclick="window.location.reload()" class="mt-4 px-4 py-2 bg-slate-800 hover:bg-slate-700 text-white text-xs font-semibold rounded-xl">Reload</button>
        </div>
      \`;
    }

    // Launch App
    initApp();
  </script>
</body>
</html>`
}
