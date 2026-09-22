import debug from "debug";
import express from "express";
import http from "http";
import { Pool } from "pg";
import { Server as SocketIO } from "socket.io";

type UserToFollow = {
  socketId: string;
  username: string;
};

type OnUserFollowedPayload = {
  userToFollow: UserToFollow;
  action: "FOLLOW" | "UNFOLLOW";
};

type StoredScene = {
  sceneVersion: number;
  iv: string;
  ciphertext: string;
};

const serverDebug = debug("server");
const ioDebug = debug("io");
const socketDebug = debug("socket");

require("dotenv").config(
  process.env.NODE_ENV !== "development"
    ? { path: ".env.production" }
    : { path: ".env.development" },
);

const app = express();

const port =
  process.env.PORT || (process.env.NODE_ENV !== "development" ? 80 : 3002);

/* -------------------------------------------------------------------------- */
/* PostgreSQL                                                                  */
/* -------------------------------------------------------------------------- */

const pool = new Pool({
  host: process.env.PGHOST || "127.0.0.1",
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  max: Number(process.env.PGPOOL_MAX || 10),
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

pool.on("error", (error) => {
  console.error("PostgreSQL pool error:", error);
});

/* -------------------------------------------------------------------------- */
/* Express                                                                    */
/* -------------------------------------------------------------------------- */

app.use(express.json({ limit: "25mb" }));

app.use(express.static("public"));

app.get("/", (_req, res) => {
  res.send("Excalidraw collaboration server is up :)");
});

/* -------------------------------------------------------------------------- */
/* PostgreSQL helpers                                                         */
/* -------------------------------------------------------------------------- */

const isValidRoomId = (roomId: string): boolean => {
  return (
    typeof roomId === "string" && roomId.length >= 1 && roomId.length <= 512
  );
};

const isValidStoredScene = (scene: unknown): scene is StoredScene => {
  if (!scene || typeof scene !== "object") {
    return false;
  }

  const value = scene as Record<string, unknown>;

  return (
    typeof value.sceneVersion === "number" &&
    Number.isFinite(value.sceneVersion) &&
    typeof value.iv === "string" &&
    value.iv.length > 0 &&
    typeof value.ciphertext === "string" &&
    value.ciphertext.length > 0
  );
};

const getScene = async (roomId: string): Promise<StoredScene | null> => {
  const result = await pool.query(
    `
      SELECT
        scene_version,
        scene_data
      FROM public.collaboration_rooms
      WHERE room_id = $1
      LIMIT 1
    `,
    [roomId],
  );

  if (result.rowCount === 0) {
    return null;
  }

  const row = result.rows[0];

  try {
    const scene = JSON.parse(row.scene_data);

    if (!isValidStoredScene(scene)) {
      throw new Error("Invalid stored scene");
    }

    return scene;
  } catch (error) {
    console.error(`Invalid scene data for room ${roomId}:`, error);

    return null;
  }
};

const saveScene = async (roomId: string, scene: StoredScene): Promise<void> => {
  const sceneData = JSON.stringify(scene);

  await pool.query(
    `
      INSERT INTO public.collaboration_rooms (
        room_id,
        scene_data,
        scene_version,
        created_at,
        updated_at
      )
      VALUES (
        $1,
        $2,
        $3,
        NOW(),
        NOW()
      )
      ON CONFLICT (room_id)
      DO UPDATE SET
        scene_data = EXCLUDED.scene_data,
        scene_version = EXCLUDED.scene_version,
        updated_at = NOW()
    `,
    [roomId, sceneData, scene.sceneVersion],
  );
};

/* -------------------------------------------------------------------------- */
/* Collaboration persistence API                                              */
/* -------------------------------------------------------------------------- */

app.get("/api/collab/rooms/:roomId", async (req, res) => {
  const { roomId } = req.params;

  if (!isValidRoomId(roomId)) {
    return res.status(400).json({
      error: "Invalid room ID",
    });
  }

  try {
    const scene = await getScene(roomId);

    if (!scene) {
      return res.status(404).json({
        error: "Room scene not found",
      });
    }

    return res.status(200).json(scene);
  } catch (error) {
    console.error(`Failed to load collaboration room ${roomId}:`, error);

    return res.status(500).json({
      error: "Failed to load collaboration room",
    });
  }
});

app.put("/api/collab/rooms/:roomId", async (req, res) => {
  const { roomId } = req.params;

  if (!isValidRoomId(roomId)) {
    return res.status(400).json({
      error: "Invalid room ID",
    });
  }

  if (!isValidStoredScene(req.body)) {
    return res.status(400).json({
      error: "Invalid scene payload",
    });
  }

  try {
    await saveScene(roomId, req.body);

    return res.status(200).json(req.body);
  } catch (error) {
    console.error(`Failed to save collaboration room ${roomId}:`, error);

    return res.status(500).json({
      error: "Failed to save collaboration room",
    });
  }
});

/* -------------------------------------------------------------------------- */
/* HTTP server                                                                */
/* -------------------------------------------------------------------------- */

const server = http.createServer(app);

server.listen(port, () => {
  serverDebug(`listening on port ${port}`);
});

/* -------------------------------------------------------------------------- */
/* Socket.IO                                                                  */
/* -------------------------------------------------------------------------- */

try {
  const io = new SocketIO(server, {
    transports: ["websocket", "polling"],
    cors: {
      allowedHeaders: ["Content-Type", "Authorization"],
      origin: process.env.CORS_ORIGIN || "*",
      credentials: true,
    },
    allowEIO3: true,
  });

  io.on("connection", (socket) => {
    ioDebug("connection established!");

    io.to(`${socket.id}`).emit("init-room");

    socket.on("join-room", async (roomID) => {
      if (!isValidRoomId(roomID)) {
        socketDebug(`${socket.id} attempted to join invalid room ${roomID}`);
        return;
      }

      socketDebug(`${socket.id} has joined ${roomID}`);

      await socket.join(roomID);

      const sockets = await io.in(roomID).fetchSockets();

      if (sockets.length <= 1) {
        io.to(`${socket.id}`).emit("first-in-room");
      } else {
        socketDebug(`${socket.id} new-user emitted to room ${roomID}`);

        socket.broadcast.to(roomID).emit("new-user", socket.id);
      }

      io.in(roomID).emit(
        "room-user-change",
        sockets.map((socket) => socket.id),
      );
    });

    socket.on(
      "server-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        socketDebug(`${socket.id} sends update to ${roomID}`);

        socket.broadcast.to(roomID).emit("client-broadcast", encryptedData, iv);
      },
    );

    socket.on(
      "server-volatile-broadcast",
      (roomID: string, encryptedData: ArrayBuffer, iv: Uint8Array) => {
        socketDebug(`${socket.id} sends volatile update to ${roomID}`);

        socket.volatile.broadcast
          .to(roomID)
          .emit("client-broadcast", encryptedData, iv);
      },
    );

    socket.on("user-follow", async (payload: OnUserFollowedPayload) => {
      const roomID = `follow@${payload.userToFollow.socketId}`;

      switch (payload.action) {
        case "FOLLOW": {
          await socket.join(roomID);

          const sockets = await io.in(roomID).fetchSockets();

          const followedBy = sockets.map((socket) => socket.id);

          io.to(payload.userToFollow.socketId).emit(
            "user-follow-room-change",
            followedBy,
          );

          break;
        }

        case "UNFOLLOW": {
          await socket.leave(roomID);

          const sockets = await io.in(roomID).fetchSockets();

          const followedBy = sockets.map((socket) => socket.id);

          io.to(payload.userToFollow.socketId).emit(
            "user-follow-room-change",
            followedBy,
          );

          break;
        }
      }
    });

    socket.on("disconnecting", async () => {
      socketDebug(`${socket.id} has disconnected`);

      for (const roomID of Array.from(socket.rooms)) {
        const otherClients = (await io.in(roomID).fetchSockets()).filter(
          (_socket) => _socket.id !== socket.id,
        );

        const isFollowRoom = roomID.startsWith("follow@");

        if (!isFollowRoom && otherClients.length > 0) {
          socket.broadcast.to(roomID).emit(
            "room-user-change",
            otherClients.map((socket) => socket.id),
          );
        }

        if (isFollowRoom && otherClients.length === 0) {
          const socketId = roomID.replace("follow@", "");

          io.to(socketId).emit("broadcast-unfollow");
        }
      }
    });

    socket.on("disconnect", () => {
      socket.removeAllListeners();
      socket.disconnect();
    });
  });
} catch (error) {
  console.error(error);
}

/* -------------------------------------------------------------------------- */
/* Graceful shutdown                                                          */
/* -------------------------------------------------------------------------- */

const shutdown = async () => {
  serverDebug("Shutting down collaboration server...");

  try {
    await pool.end();
  } catch (error) {
    console.error("Failed to close PostgreSQL pool:", error);
  }

  server.close(() => {
    process.exit(0);
  });
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
