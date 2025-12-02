const express = require('express');
const { Pool } = require('pg');
const redis = require('redis');

const app = express();
app.use(express.json());

const databaseUrl = process.env.DATABASE_URL || 'postgresql://crmuser:crmpass@localhost:5432/internalcrm';
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

const pool = new Pool({
  connectionString: databaseUrl
});

let redisClient;

async function initRedis() {
  redisClient = redis.createClient({ url: redisUrl });
  await redisClient.connect();
}

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS notifications (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL,
      type VARCHAR(50) NOT NULL,
      channel VARCHAR(50) NOT NULL,
      subject VARCHAR(255),
      message TEXT NOT NULL,
      status VARCHAR(20) DEFAULT 'pending',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      sent_at TIMESTAMP
    )
  `);
  
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id)
  `);
  
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications(status)
  `);
}

async function processQueue() {
  while (true) {
    try {
      const notificationId = await redisClient.brPop('notification_queue', 5);
      if (notificationId && notificationId.element) {
        const id = parseInt(notificationId.element);
        const result = await pool.query(`
          SELECT id, user_id, type, channel, subject, message
          FROM notifications
          WHERE id = $1
        `, [id]);
        
        if (result.rows.length > 0) {
          const notif = result.rows[0];
          await sendNotification(notif);
          
          await pool.query(`
            UPDATE notifications
            SET status = 'sent', sent_at = CURRENT_TIMESTAMP
            WHERE id = $1
          `, [id]);
        }
      }
    } catch (error) {
      console.error('error processing queue:', error);
    }
  }
}

async function sendNotification(notif) {
  if (notif.channel === 'email') {
    console.log(`sending email to user ${notif.user_id}: ${notif.subject}`);
  } else if (notif.channel === 'slack') {
    console.log(`sending slack to user ${notif.user_id}: ${notif.message}`);
  } else if (notif.channel === 'telegram') {
    console.log(`sending telegram to user ${notif.user_id}: ${notif.message}`);
  }
}

app.post('/notifications', async (req, res) => {
  try {
    const { user_id, type, channel, subject, message } = req.body;
    
    if (!user_id || !type || !channel || !message) {
      return res.status(400).json({ error: 'missing required fields' });
    }
    
    if (!['email', 'slack', 'telegram'].includes(channel)) {
      return res.status(400).json({ error: 'invalid channel' });
    }
    
    const result = await pool.query(`
      INSERT INTO notifications (user_id, type, channel, subject, message, status)
      VALUES ($1, $2, $3, $4, $5, 'pending')
      RETURNING id, user_id, type, channel, subject, message, status, created_at
    `, [user_id, type, channel, subject || null, message]);
    
    const notification = result.rows[0];
    
    await redisClient.lPush('notification_queue', notification.id.toString());
    
    res.json({
      id: notification.id,
      user_id: notification.user_id,
      type: notification.type,
      channel: notification.channel,
      subject: notification.subject,
      message: notification.message,
      status: notification.status,
      created_at: notification.created_at.toISOString()
    });
  } catch (error) {
    console.error('error creating notification:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/notifications/:id', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT id, user_id, type, channel, subject, message, status, created_at, sent_at
      FROM notifications
      WHERE id = $1
    `, [req.params.id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'notification not found' });
    }
    
    const notif = result.rows[0];
    res.json({
      id: notif.id,
      user_id: notif.user_id,
      type: notif.type,
      channel: notif.channel,
      subject: notif.subject,
      message: notif.message,
      status: notif.status,
      created_at: notif.created_at.toISOString(),
      sent_at: notif.sent_at ? notif.sent_at.toISOString() : null
    });
  } catch (error) {
    console.error('error getting notification:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/notifications', async (req, res) => {
  try {
    const userId = req.query.user_id;
    const limit = parseInt(req.query.limit) || 100;
    const offset = parseInt(req.query.offset) || 0;
    
    let query = 'SELECT id, user_id, type, channel, subject, message, status, created_at, sent_at FROM notifications';
    let params = [];
    
    if (userId) {
      query += ' WHERE user_id = $1';
      params.push(userId);
      query += ` ORDER BY created_at DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
      params.push(limit, offset);
    } else {
      query += ` ORDER BY created_at DESC LIMIT $1 OFFSET $2`;
      params.push(limit, offset);
    }
    
    const result = await pool.query(query, params);
    
    const notifications = result.rows.map(notif => ({
      id: notif.id,
      user_id: notif.user_id,
      type: notif.type,
      channel: notif.channel,
      subject: notif.subject,
      message: notif.message,
      status: notif.status,
      created_at: notif.created_at.toISOString(),
      sent_at: notif.sent_at ? notif.sent_at.toISOString() : null
    }));
    
    res.json({ notifications, count: notifications.length });
  } catch (error) {
    console.error('error listing notifications:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

async function start() {
  await initRedis();
  await initDb();
  processQueue();
  
  const port = process.env.PORT || 3000;
  app.listen(port, () => {
    console.log(`notifications service running on port ${port}`);
  });
}

start().catch(console.error);

