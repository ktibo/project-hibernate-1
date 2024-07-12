package com.game.repository;

import com.game.entity.Player;
import com.game.entity.Profession;
import com.game.entity.Race;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.springframework.stereotype.Repository;

import javax.annotation.PreDestroy;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

@Repository(value = "db")
public class PlayerRepositoryDB implements IPlayerRepository {

    private final SessionFactory sessionFactory;

    public PlayerRepositoryDB() {

        try {
            sessionFactory = new Configuration()
                    .addAnnotatedClass(Player.class)
                    .buildSessionFactory();
        } catch (HibernateException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public List<Player> getAll(int pageNumber, int pageSize) {

        List<Player> list = new ArrayList<>();

        try (Session session = sessionFactory.openSession()) {
            NativeQuery sqlQuery = session.createSQLQuery("select * from rpg.player limit :pageSize offset :offset");
            sqlQuery.setParameter("pageSize", pageSize);
            sqlQuery.setParameter("offset", pageNumber*pageSize);
            List<Object[]> queryList = sqlQuery.list();
            for (Object[] arr : queryList) {
                Player player = new Player(
                    ((BigInteger) arr[0]).longValue(),
                    (String) arr[4],
                    (String) arr[7],
                    Race.values()[(int)arr[6]],
                    Profession.values()[(int)arr[5]],
                    (Date)arr[2],
                    (Boolean)arr[1],
                    (Integer)arr[3]
                );
                list.add(player);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }

    @Override
    public int getAllCount() {

        try (Session session = sessionFactory.openSession()) {
            Query<Long> getAllCount = session.createNamedQuery("getAllCount", Long.class);
            return getAllCount.uniqueResult().intValue();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    @Override
    public Player save(Player player) {

        Session session = sessionFactory.openSession();
        try {
            session.beginTransaction();
            Query<Long> query = session.createQuery("select max(p.id) from Player p");
            Long maxID = query.uniqueResult();
            player.setId(maxID+1);
            session.persist(player);
            session.getTransaction().commit();
            return player;
        } catch (Exception e) {
            session.getTransaction().rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }

        return null;

    }

    @Override
    public Player update(Player player) {

        Session session = sessionFactory.openSession();
        try {
            session.beginTransaction();
            session.merge(player);
            session.getTransaction().commit();
            return player;
        } catch (Exception e) {
            session.getTransaction().rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }

        return null;

    }

    @Override
    public Optional<Player> findById(long id) {

        try (Session session = sessionFactory.openSession()) {
            Query<Player> query = session.createQuery("from Player where id = :id", Player.class);
            query.setParameter("id", id);
            Player player = query.uniqueResult();
            return Optional.ofNullable(player);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return Optional.empty();

    }

    @Override
    public void delete(Player player) {

        Session session = sessionFactory.openSession();
        try {
            session.beginTransaction();
            session.delete(player);
            session.getTransaction().commit();
        } catch (Exception e) {
            session.getTransaction().rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }

    }

    @PreDestroy
    public void beforeStop() {
        sessionFactory.close();
    }
}