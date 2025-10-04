'use client'

import { useState, useRef, useEffect } from "react"
import { useWebSocket } from "../context/SocketProvider"
import styles from './page.module.css'

export default function Page() {
  const { messages, sendMessage, isConnected } = useWebSocket()
  const [message, setMessage] = useState("")
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
  }

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const handleSubmit = (e : React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    if (message.trim() && isConnected) {
      sendMessage(message)
      setMessage("")
    }
  }

  const handleKeyPress = (e:React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit(e as unknown as React.FormEvent<HTMLFormElement>)
    }
  }

  return (
    <div className={styles.container}>
      {/* Header */}
      <header className={styles.header}>
        <div className={styles.headerContent}>
          <h1 className={styles.title}>Global Chat</h1>
          <div className={styles.connectionStatus}>
            <div className={`${styles.statusDot} ${isConnected ? styles.connected : styles.disconnected}`} />
            <span>{isConnected ? 'Connected' : 'Disconnected'}</span>
          </div>
        </div>
      </header>

      {/* Messages Container */}
      <main className={styles.main}>
        <div className={styles.messagesContainer}>
          {messages.length === 0 ? (
            <div className={styles.emptyState}>
              <div className={styles.emptyIcon}>💬</div>
              <h3>No messages yet</h3>
              <p>Start a conversation by sending the first message!</p>
            </div>
          ) : (
            <div className={styles.messagesList}>
              {messages.map((msg, index) => (
                <div key={index} className={styles.messageBubble}>
                  <div className={styles.messageContent}>{msg}</div>
                  {/* <div className={styles.messageTime}>
                    {new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </div> */}
                </div>
              ))}
              <div ref={messagesEndRef} />
            </div>
          )}
        </div>
      </main>

      {/* Input Area */}
      <footer className={styles.footer}>
        <form onSubmit={handleSubmit} className={styles.inputForm}>
          <div className={styles.inputContainer}>
            <input
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              onKeyPress={handleKeyPress}
              type="text"
              placeholder={isConnected ? "Type a message..." : "Connecting..."}
              className={styles.chatInput}
              disabled={!isConnected}
              maxLength={500}
            />
            <div className={styles.inputActions}>
              <span className={styles.charCount}>{message.length}/500</span>
              <button 
                type="submit" 
                className={styles.sendButton}
                disabled={!message.trim() || !isConnected}
              >
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
                  <path d="M2 21L23 12L2 3V10L17 12L2 14V21Z" fill="currentColor"/>
                </svg>
              </button>
            </div>
          </div>
        </form>
      </footer>
    </div>
  )
}