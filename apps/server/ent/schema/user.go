package schema

import (
    "entgo.io/ent"
    "entgo.io/ent/schema/field"
	"github.com/google/uuid"
	"time"
)

// Message holds the schema definition for the Message entity.
type Message struct {
    ent.Schema
}

// Fields of the Message.
func (Message) Fields() []ent.Field {
    return []ent.Field{
        field.UUID("id", uuid.UUID{}).Default(uuid.New),   // Prisma uuid()
        field.String("text"),                              // Prisma String
        field.Time("created_at").Default(time.Now),        // Prisma DateTime @default(now())
    }
}

// Edges of the Message.
func (Message) Edges() []ent.Edge {
    return nil
}
