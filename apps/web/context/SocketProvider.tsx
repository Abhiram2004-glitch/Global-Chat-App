"use client";
import React, { useCallback, useContext, useEffect, useRef, useState } from "react";

interface WebSocketProviderProps {
  children?: React.ReactNode;
}

interface IWebSocketContext {
  sendMessage: (msg: string) => void;
  messages: string[];
  isConnected: boolean;
}

const WebSocketContext = React.createContext<IWebSocketContext | null>(null);

export const useWebSocket = () => {
  const state = useContext(WebSocketContext);
  if (!state) throw new Error(`WebSocket context is undefined`);
  return state;
};

export const WebSocketProvider: React.FC<WebSocketProviderProps> = ({ children }) => {
  const [messages, setMessages] = useState<string[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const ws = useRef<WebSocket | null>(null);
  const reconnectTimeout = useRef<NodeJS.Timeout | undefined>(undefined);
  const onMessageReceived = useCallback((msg: string) => {
    console.log("Message received from server:", msg);
  }, []);
  const connect = useCallback(() => {
    try {
      // Connect to your Go WebSocket server
      const socket = new WebSocket("ws://localhost:8080/subscribe");

      socket.onopen = () => {
        console.log("WebSocket connected");
        setIsConnected(true);
      };

      socket.onmessage = (event) => {
        console.log("Message received:", event.data);
        onMessageReceived(event.data);

        try {   
          
          const data = JSON.parse(event.data);
          // Assuming your Go server sends: {"message": "text"}
          setMessages((prev) => [...prev, data.message || event.data]);
        } catch (e) {
          // If not JSON, just use raw data
          setMessages((prev) => [...prev, event.data]);
        }
      };

      socket.onerror = (error) => {
        console.error("WebSocket error:", error);
      };

      socket.onclose = () => {
        console.log("WebSocket disconnected");
        setIsConnected(false);
        
        // Auto-reconnect after 3 seconds
        reconnectTimeout.current = setTimeout(() => {
          console.log("Attempting to reconnect...");
          connect();
        }, 3000);
      };

      ws.current = socket;
    } catch (error) {
      console.error("Failed to connect:", error);
    }
  }, [onMessageReceived]);

  const sendMessage = useCallback((msg: string) => {
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      // For the Go server's publish endpoint, use HTTP POST
      fetch("http://localhost:8080/publish", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ message: msg }),
      }).catch((error) => {
        console.error("Failed to send message:", error);
      });
    } else {
      console.warn("WebSocket is not connected");
    }
  }, []);

  useEffect(() => {
    connect();

    return () => {
      if (reconnectTimeout.current) {
        clearTimeout(reconnectTimeout.current);
      }
      if (ws.current) {
        ws.current.close();
      }
    };
  }, [connect]);

  return (
    <WebSocketContext.Provider value={{ sendMessage, messages, isConnected }}>
      {children}
    </WebSocketContext.Provider>
  );
}