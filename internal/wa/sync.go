package wa

import (
	"database/sql"
	"time"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// handleMessage processes real-time incoming messages and persists them.
func (c *Client) handleMessage(msg *events.Message) {
	chatJID := msg.Info.Chat.String()
	sender := msg.Info.Sender.User
	content := extractTextContent(msg.Message)
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	if content == "" && mediaType == "" {
		return
	}

	// Ensure we have a per-sender chat entry with a friendly name for name lookups
	if sender != "" {
		indiv := types.JID{User: sender, Server: "s.whatsapp.net"}
		var existing sql.NullString
		_ = c.Store.Messages.QueryRow("SELECT name FROM chats WHERE jid = ?", indiv.String()).Scan(&existing)
		if !existing.Valid {
			resolved := c.resolvePreferredName(indiv)
			_, _ = c.Store.Messages.Exec("INSERT INTO chats (jid, name) VALUES (?, ?)", indiv.String(), resolved)
		} else if existing.String == "" {
			resolved := c.resolvePreferredName(indiv)
			if resolved != "" {
				_, _ = c.Store.Messages.Exec("UPDATE chats SET name = ? WHERE jid = ?", resolved, indiv.String())
			}
		}
	}

	name := c.getChatName(msg.Info.Chat, chatJID, nil, sender)
	if _, err := c.Store.Messages.Exec("INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)", chatJID, name, msg.Info.Timestamp); err != nil {
		c.Logger.Warn("failed to upsert chat", "jid", chatJID, "err", err)
	}

	if _, err := c.Store.Messages.Exec(`INSERT OR REPLACE INTO messages
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.Info.ID, chatJID, sender, content, msg.Info.Timestamp, msg.Info.IsFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	); err != nil {
		c.Logger.Warn("failed to store message", "id", msg.Info.ID, "chat_jid", chatJID, "err", err)
	}
}

// handleHistorySync persists conversations and messages received during a history sync.
func (c *Client) handleHistorySync(hs *events.HistorySync) {
	if hs == nil || hs.Data.Conversations == nil {
		return
	}

	synced := 0
	for _, conv := range hs.Data.Conversations {
		if conv == nil || conv.ID == nil {
			continue
		}

		chatJID := *conv.ID
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			c.Logger.Warn("history sync: bad JID", "jid", chatJID, "err", err)
			continue
		}

		name := c.getChatName(jid, chatJID, conv, "")

		if len(conv.Messages) > 0 && conv.Messages[0] != nil && conv.Messages[0].Message != nil {
			ts := conv.Messages[0].Message.GetMessageTimestamp()
			if ts != 0 {
				t := time.Unix(int64(ts), 0)
				if _, err := c.Store.Messages.Exec("INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)", chatJID, name, t); err != nil {
					c.Logger.Warn("history sync: failed to upsert chat", "jid", chatJID, "err", err)
				}
			}
		}

		for _, m := range conv.Messages {
			if m == nil || m.Message == nil {
				continue
			}

			// Use whatsmeow's ParseWebMessage for sender resolution.
			// It handles participant priority, LID mapping, own-JID for fromMe,
			// message edits, and ephemeral message unwrapping.
			evt, parseErr := c.WA.ParseWebMessage(jid, m.Message)

			var sender, id string
			var fromMe bool
			var ts time.Time
			var msg *waE2E.Message

			if parseErr == nil {
				sender = evt.Info.Sender.User
				id = evt.Info.ID
				fromMe = evt.Info.IsFromMe
				ts = evt.Info.Timestamp
				msg = evt.Message
			} else {
				// ParseWebMessage can fail when participant data is missing
				// for group messages. Store with empty sender, don't skip.
				c.Logger.Debug("history sync: ParseWebMessage fallback", "err", parseErr, "chat", chatJID)
				if m.Message.Key != nil && m.Message.Key.ID != nil {
					id = *m.Message.Key.ID
				}
				if m.Message.Key != nil && m.Message.Key.FromMe != nil {
					fromMe = *m.Message.Key.FromMe
				}
				rawTS := m.Message.GetMessageTimestamp()
				if rawTS == 0 {
					continue
				}
				ts = time.Unix(int64(rawTS), 0)
				msg = m.Message.Message
			}

			var text string
			if msg != nil {
				text = extractTextContent(msg)
			}

			mt, fn, u, mk, sha, enc, fl := "", "", "", ([]byte)(nil), ([]byte)(nil), ([]byte)(nil), uint64(0)
			if msg != nil {
				mt, fn, u, mk, sha, enc, fl = extractMediaInfo(msg)
			}

			if text == "" && mt == "" {
				c.Logger.Debug("history sync: skipping non-text/non-media message", "id", id, "chat", chatJID)
				continue
			}

			// Upsert a per-sender chat entry for name resolution
			if !fromMe && sender != "" {
				indiv := types.JID{User: sender, Server: "s.whatsapp.net"}
				var existing sql.NullString
				_ = c.Store.Messages.QueryRow("SELECT name FROM chats WHERE jid = ?", indiv.String()).Scan(&existing)
				if !existing.Valid {
					resolved := c.resolvePreferredName(indiv)
					_, _ = c.Store.Messages.Exec("INSERT INTO chats (jid, name) VALUES (?, ?)", indiv.String(), resolved)
				} else if existing.String == "" {
					resolved := c.resolvePreferredName(indiv)
					if resolved != "" {
						_, _ = c.Store.Messages.Exec("UPDATE chats SET name = ? WHERE jid = ?", resolved, indiv.String())
					}
				}
			}

			if _, err := c.Store.Messages.Exec(`INSERT OR REPLACE INTO messages
				(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, id, chatJID, sender, text, ts, fromMe, mt, fn, u, mk, sha, enc, fl); err != nil {
				c.Logger.Warn("history sync: failed to store message", "id", id, "chat_jid", chatJID, "err", err)
				continue
			}
			synced++
		}
	}

	c.Logger.Info("history sync persisted messages", "count", synced)
}
