package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
)

func seed(ctx context.Context, c *authzed.Client) error {
	schema, err := os.ReadFile("schema.zed")
	if err != nil {
		return err
	}
	if _, err := c.WriteSchema(ctx, &v1.WriteSchemaRequest{Schema: string(schema)}); err != nil {
		return fmt.Errorf("write schema: %w", err)
	}

	r := rand.New(rand.NewSource(42))
	var rels []*v1.Relationship
	for g := range groups {
		for range 10 {
			rels = append(rels, rel("group", fmt.Sprint(g), "member", "user", fmt.Sprint(r.Intn(users)), ""))
		}
		if g%6 == 0 {
			rels = append(rels, rel("group", fmt.Sprint(g), "member", "group", fmt.Sprint((g+1)%groups), "member"))
		}
	}
	for n := range resources {
		rels = append(rels, rel("resource", fmt.Sprint(n), "admin", "group", fmt.Sprint(r.Intn(groups)), "member"))
		for range 3 {
			rels = append(rels, rel("resource", fmt.Sprint(n), "viewer", "group", fmt.Sprint(r.Intn(groups)), "member"))
		}
		for range 2 {
			rels = append(rels, rel("resource", fmt.Sprint(n), "viewer", "user", fmt.Sprint(r.Intn(users)), ""))
		}
	}

	seen := map[string]bool{}
	unique := rels[:0]
	for _, rl := range rels {
		k := rl.Resource.ObjectType + rl.Resource.ObjectId + rl.Relation + rl.Subject.Object.ObjectType + rl.Subject.Object.ObjectId + rl.Subject.OptionalRelation
		if !seen[k] {
			seen[k] = true
			unique = append(unique, rl)
		}
	}
	rels = unique

	const batch = 1000
	for i := 0; i < len(rels); i += batch {
		end := min(i+batch, len(rels))
		updates := make([]*v1.RelationshipUpdate, 0, end-i)
		for _, rl := range rels[i:end] {
			updates = append(updates, &v1.RelationshipUpdate{Operation: v1.RelationshipUpdate_OPERATION_TOUCH, Relationship: rl})
		}
		if _, err := c.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{Updates: updates}); err != nil {
			return fmt.Errorf("write batch %d: %w", i, err)
		}
	}
	log.Printf("seeded %d relationships", len(rels))
	return nil
}

func rel(resType, resID, relation, subType, subID, subRel string) *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{ObjectType: resType, ObjectId: resID},
		Relation: relation,
		Subject: &v1.SubjectReference{
			Object:           &v1.ObjectReference{ObjectType: subType, ObjectId: subID},
			OptionalRelation: subRel,
		},
	}
}
