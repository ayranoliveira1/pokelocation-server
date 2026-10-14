package groups

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Travas: o teto de 25 grupos de um usuário é conferido sob o advisory lock dele, e o de 50 membros sob o FOR UPDATE
// do grupo. Ninguém espera um grupo segurando o lock de usuário, então não há ciclo.
type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool} }

const lockUser = `SELECT pg_advisory_xact_lock(hashtextextended('groups:' || $1::text, 0))`

// Queries com o plano conferido em indexes_integration_test.go.
const (
	// amigos aceitos de $1, sem bloqueio em nenhum sentido, entre os $2
	qFriendsOK = `SELECT count(*) FROM unnest($2::uuid[]) m
		WHERE EXISTS (SELECT 1 FROM friendships WHERE status = 'accepted'
			AND least(requester_id, addressee_id) = least($1::uuid, m)
			AND greatest(requester_id, addressee_id) = greatest($1::uuid, m))
		AND NOT EXISTS (SELECT 1 FROM blocks
			WHERE (blocker_id = $1 AND blocked_id = m) OR (blocker_id = m AND blocked_id = $1))`
	qGroupsFull = `SELECT EXISTS (SELECT 1 FROM group_members WHERE user_id = ANY($1::uuid[])
		GROUP BY user_id HAVING count(*) >= $2)`
	qInsertGroup   = `INSERT INTO groups (owner_id) VALUES ($1) RETURNING id`
	qInsertMembers = `INSERT INTO group_members (group_id, user_id) SELECT $1, u FROM unnest($2::uuid[]) u`
	qLockGroup     = `SELECT owner_id FROM groups WHERE id = $1 FOR UPDATE`
	qIsMember      = `SELECT EXISTS (SELECT 1 FROM group_members WHERE group_id = $1 AND user_id = $2)`
	qMemberCount   = `SELECT count(*) FROM group_members WHERE group_id = $1`
	qAddMember     = `INSERT INTO group_members (group_id, user_id) VALUES ($1, $2)`
	qRemoveMember  = `DELETE FROM group_members WHERE group_id = $1 AND user_id = $2`
	qMembers       = `SELECT user_id::text FROM group_members WHERE group_id = $1`
	qUserGroups    = `SELECT group_id::text FROM group_members WHERE user_id = $1 ORDER BY group_id`
	qDeleteGroup   = `DELETE FROM groups WHERE id = $1`
	// o dono é sempre o mais antigo: quem herda é o próximo mais antigo
	qTransfer = `UPDATE groups SET owner_id = (SELECT user_id FROM group_members WHERE group_id = $1
		ORDER BY joined_at, user_id LIMIT 1) WHERE id = $1`
	qList = `SELECT g.id, g.owner_id, g.created_at,
			array_agg(m.user_id::text ORDER BY m.joined_at, m.user_id <> g.owner_id, m.user_id),
			array_agg(u.username ORDER BY m.joined_at, m.user_id <> g.owner_id, m.user_id)
		FROM group_members me
		JOIN groups g ON g.id = me.group_id
		JOIN group_members m ON m.group_id = g.id
		JOIN users u ON u.id = m.user_id
		WHERE me.user_id = $1
		GROUP BY g.id
		ORDER BY g.created_at, g.id
		LIMIT $2`
	qGet = `SELECT g.id, g.owner_id, g.created_at,
			array_agg(m.user_id::text ORDER BY m.joined_at, m.user_id <> g.owner_id, m.user_id),
			array_agg(u.username ORDER BY m.joined_at, m.user_id <> g.owner_id, m.user_id)
		FROM group_members me
		JOIN groups g ON g.id = me.group_id
		JOIN group_members m ON m.group_id = g.id
		JOIN users u ON u.id = m.user_id
		WHERE me.user_id = $1 AND me.group_id = $2
		GROUP BY g.id`
)

// lockUsers trava em ordem fixa (evita deadlock entre duas criações com os mesmos usuários).
func lockUsers(ctx context.Context, tx pgx.Tx, users []string) error {
	users = slices.Sorted(slices.Values(users))
	b := &pgx.Batch{}
	for _, u := range users {
		b.Queue(lockUser, u)
	}
	return tx.SendBatch(ctx, b).Close()
}

func friendsOK(ctx context.Context, tx pgx.Tx, me string, others []string) error {
	var n int
	if err := tx.QueryRow(ctx, qFriendsOK, me, others).Scan(&n); err != nil {
		return err
	}
	if n != len(others) {
		return ErrForbidden
	}
	return nil
}

func groupsFull(ctx context.Context, tx pgx.Tx, users []string) error {
	var full bool
	if err := tx.QueryRow(ctx, qGroupsFull, users, maxGroups).Scan(&full); err != nil {
		return err
	}
	if full {
		return ErrTooManyGroups
	}
	return nil
}

// create: members já sem repetidos e sem o dono. Devolve o id e todos os membros.
func (s *Store) create(ctx context.Context, owner string, members []string) (id string, users []string, err error) {
	users = append([]string{owner}, members...)
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockUsers(ctx, tx, users); err != nil {
			return err
		}
		if len(members) > 0 {
			if err := friendsOK(ctx, tx, owner, members); err != nil {
				return err
			}
		}
		if err := groupsFull(ctx, tx, users); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, qInsertGroup, owner).Scan(&id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, qInsertMembers, id, users)
		return err
	})
	return id, users, err
}

// lockMember trava o grupo e confere que me é membro (senão ErrNotFound, sem revelar se existe).
func lockMember(ctx context.Context, tx pgx.Tx, group, me string) (owner string, err error) {
	err = tx.QueryRow(ctx, qLockGroup, group).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	var member bool
	if err := tx.QueryRow(ctx, qIsMember, group, me).Scan(&member); err != nil {
		return "", err
	}
	if !member {
		return "", ErrNotFound
	}
	return owner, nil
}

func currentMembers(ctx context.Context, tx pgx.Tx, group string) ([]string, error) {
	rows, err := tx.Query(ctx, qMembers, group)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// add devolve os membros atuais; nil se nada mudou (já era membro).
func (s *Store) add(ctx context.Context, me, group, user string) (users []string, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := lockMember(ctx, tx, group, me); err != nil {
			return err
		}
		var already bool
		if err := tx.QueryRow(ctx, qIsMember, group, user).Scan(&already); err != nil || already {
			return err
		}
		if err := friendsOK(ctx, tx, me, []string{user}); err != nil {
			return err
		}
		var n int
		if err := tx.QueryRow(ctx, qMemberCount, group).Scan(&n); err != nil {
			return err
		}
		if n >= maxMembers {
			return ErrTooManyMembers
		}
		if _, err := tx.Exec(ctx, lockUser, user); err != nil {
			return err
		}
		if err := groupsFull(ctx, tx, []string{user}); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, qAddMember, group, user); err != nil {
			return err
		}
		users, err = currentMembers(ctx, tx, group)
		return err
	})
	return users, err
}

// remove devolve os membros que ficaram mais o que saiu; nil se nada mudou (target já fora).
func (s *Store) remove(ctx context.Context, me, group, target string) (users []string, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		owner, err := lockMember(ctx, tx, group, me)
		if err != nil {
			return err
		}
		if target != me && owner != me {
			return ErrForbidden
		}
		users, err = leaveLocked(ctx, tx, group, owner, target)
		return err
	})
	return users, err
}

// leaveLocked: com o grupo já travado, tira target; o dono saindo passa ao mais antigo e o último apaga o grupo.
func leaveLocked(ctx context.Context, tx pgx.Tx, group, owner, target string) ([]string, error) {
	tag, err := tx.Exec(ctx, qRemoveMember, group, target)
	if err != nil || tag.RowsAffected() == 0 {
		return nil, err
	}
	left, err := currentMembers(ctx, tx, group)
	if err != nil {
		return nil, err
	}
	switch {
	case len(left) == 0:
		_, err = tx.Exec(ctx, qDeleteGroup, group)
	case target == owner:
		_, err = tx.Exec(ctx, qTransfer, group)
	}
	return append(left, target), err
}

// Change: um grupo que mudou e quem avisar (os membros que ficaram e quem saiu).
type Change struct {
	Group string
	Users []string
}

// LeaveAllTx tira user de todos os grupos dentro de tx (vínculo Google: a conta volta limpa ao dono do email). Os
// grupos são travados em ordem de id, como um DELETE de membro por vez. Quem chama avisa depois do commit.
func LeaveAllTx(ctx context.Context, tx pgx.Tx, user string) ([]Change, error) {
	rows, err := tx.Query(ctx, qUserGroups, user)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	var out []Change
	for _, g := range ids {
		var owner string
		err := tx.QueryRow(ctx, qLockGroup, g).Scan(&owner)
		if errors.Is(err, pgx.ErrNoRows) {
			continue // o último membro saiu entre a lista e a trava: o grupo já não existe
		}
		if err != nil {
			return nil, err
		}
		users, err := leaveLocked(ctx, tx, g, owner, user)
		if err != nil {
			return nil, err
		}
		if users != nil {
			out = append(out, Change{g, users})
		}
	}
	return out, nil
}

func scanGroup(row pgx.CollectableRow) (Group, error) {
	var g Group
	var at time.Time
	var ids []string
	var names []*string
	err := row.Scan(&g.ID, &g.OwnerID, &at, &ids, &names)
	g.CreatedAt = at.UnixMilli()
	g.Members = make([]Member, len(ids))
	for i := range ids {
		g.Members[i] = Member{ids[i], names[i]}
	}
	return g, err
}

func (s *Store) list(ctx context.Context, me string) ([]Group, error) {
	rows, err := s.pool.Query(ctx, qList, me, listLimit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanGroup)
}

func (s *Store) get(ctx context.Context, me, group string) (Group, error) {
	rows, err := s.pool.Query(ctx, qGet, me, group)
	if err != nil {
		return Group{}, err
	}
	g, err := pgx.CollectExactlyOneRow(rows, scanGroup)
	if errors.Is(err, pgx.ErrNoRows) {
		return Group{}, ErrNotFound
	}
	return g, err
}
